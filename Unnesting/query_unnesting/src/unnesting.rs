use crate::query_tree::QueryTree;
use crate::{Column, Expr, NodeId, RelNode};
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
struct UnnestingInfo {
    outer_refs: HashSet<Column>,
    domain: Option<RelNode>,
    cclasses: HashMap<Column, HashSet<Column>>,
    repr: HashMap<Column, Column>,
    parent: Option<Box<UnnestingInfo>>,
}

impl UnnestingInfo {
    fn new(outer_refs: HashSet<Column>, domain: Option<RelNode>) -> Self {
        Self {
            outer_refs,
            domain,
            cclasses: HashMap::new(),
            repr: HashMap::new(),
            parent: None,
        }
    }

    fn with_parent(mut self, parent: UnnestingInfo) -> Self {
        self.parent = Some(Box::new(parent));
        self
    }

    fn merge_equivalence_classes(&mut self, other: &UnnestingInfo) {
        for (col, equivalents) in &other.cclasses {
            let entry = self
                .cclasses
                .entry(col.clone())
                .or_insert_with(HashSet::new);
            entry.extend(equivalents.iter().cloned());
        }
    }

    fn create_replacement_mappings(&mut self, available_columns: &HashSet<Column>) {
        for col in &self.outer_refs {
            if self.repr.contains_key(col) {
                continue;
            }

            if let Some(equivalents) = self.cclasses.get(col) {
                for equiv_col in equivalents {
                    if available_columns.contains(equiv_col) {
                        self.repr.insert(col.clone(), equiv_col.clone());
                        break;
                    }
                }
            }
        }
    }
}

pub fn process_node(
    node: &mut RelNode,
    tree: &QueryTree,
    parent_info: Option<UnnestingInfo>,
    available_columns: &mut HashSet<Column>,
) -> UnnestingInfo {
    match node {
        RelNode::Join {
            id,
            left,
            right,
            condition,
            is_dependent,
            accessing,
        } => {
            if *is_dependent && !accessing.is_empty() {
                if try_simple_unnesting(right, accessing, condition) {
                    *is_dependent = false;
                    accessing.clear();

                    let mut local_available = available_columns.clone();
                    let left_info =
                        process_node(left, tree, parent_info.clone(), &mut local_available);
                    let right_info = process_node(right, tree, parent_info, available_columns);

                    available_columns.extend(local_available);

                    return merge_unnesting_info(left_info, right_info);
                }

                if let Some(parent) = &parent_info {
                    let mut left_available = available_columns.clone();
                    let left_info =
                        process_node(left, tree, Some(parent.clone()), &mut left_available);

                    available_columns.extend(left_available);
                }

                let (left_cols, right_free_vars) = find_correlation(left, right);
                let outer_refs: HashSet<Column> =
                    left_cols.intersection(&right_free_vars).cloned().collect();

                let domain = if !outer_refs.is_empty() {
                    Some(create_domain_node(left, &outer_refs))
                } else {
                    None
                };

                let mut info = UnnestingInfo::new(outer_refs.clone(), domain);

                if let Some(parent) = parent_info {
                    info = info.with_parent(parent);
                }

                add_equivalences_from_expr(condition, &mut info.cclasses);

                let right_info = process_node(right, tree, Some(info.clone()), available_columns);

                info.create_replacement_mappings(available_columns);

                *condition = rewrite_expr(condition, &info.repr);
                *right = decorrelate_node(right, &info);
                *is_dependent = false;
                accessing.clear();

                if let Some(domain_node) = &info.domain {
                    let domain_join_condition =
                        create_domain_join_condition(&info.outer_refs, &info.repr);
                    let domain_join = RelNode::Join {
                        id: get_next_id(),
                        left: Box::new(domain_node.clone()),
                        right: Box::new(*right.clone()),
                        condition: domain_join_condition,
                        is_dependent: false,
                        accessing: HashSet::new(),
                    };

                    *right = Box::new(domain_join);
                }

                for col in &outer_refs {
                    if let Some(new_col) = info.repr.get(col) {
                        available_columns.insert(new_col.clone());
                    }
                }

                return info;
            } else {
                let mut left_available = available_columns.clone();
                let left_info = process_node(left, tree, parent_info.clone(), &mut left_available);
                let right_info = process_node(right, tree, parent_info, available_columns);

                available_columns.extend(left_available);

                let mut merged_info = merge_unnesting_info(left_info, right_info);
                *condition = rewrite_expr(condition, &merged_info.repr);

                return merged_info;
            }
        }

        RelNode::Select {
            id,
            predicate,
            input,
        } => {
            let mut info = if let Some(parent) = parent_info {
                parent
            } else {
                UnnestingInfo::new(HashSet::new(), None)
            };

            add_equivalences_from_expr(predicate, &mut info.cclasses);

            let input_info = process_node(input, tree, Some(info.clone()), available_columns);

            info.merge_equivalence_classes(&input_info);
            info.create_replacement_mappings(available_columns);
            *predicate = rewrite_expr(predicate, &info.repr);

            return info;
        }

        RelNode::Map {
            id,
            mappings,
            input,
        } => {
            let input_info = process_node(input, tree, parent_info.clone(), available_columns);

            let mut info = if let Some(parent) = parent_info {
                parent
            } else {
                input_info.clone()
            };

            info.create_replacement_mappings(available_columns);

            for (_, expr) in mappings.iter_mut() {
                *expr = rewrite_expr(expr, &info.repr);
            }

            for (col, _) in mappings {
                available_columns.insert(col.clone());
            }

            return info;
        }

        RelNode::GroupBy {
            id,
            keys,
            aggregates,
            input,
        } => {
            let mut info = if let Some(parent) = parent_info {
                parent
            } else {
                UnnestingInfo::new(HashSet::new(), None)
            };

            let input_info = process_node(input, tree, Some(info.clone()), available_columns);
            info.merge_equivalence_classes(&input_info);

            info.create_replacement_mappings(available_columns);

            for key in keys.iter_mut() {
                if let Some(new_key) = info.repr.get(key) {
                    *key = new_key.clone();
                }
            }

            for (_, expr) in aggregates.iter_mut() {
                *expr = rewrite_expr(expr, &info.repr);
            }

            for col in &info.outer_refs {
                if let Some(new_col) = info.repr.get(col) {
                    if !keys.contains(new_col) {
                        keys.push(new_col.clone());
                    }
                }
            }

            available_columns.clear();
            for key in keys {
                available_columns.insert(key.clone());
            }
            for (col, _) in aggregates {
                available_columns.insert(col.clone());
            }

            return info;
        }

        RelNode::Table { .. } => {
            let produced = get_node_produced_columns(node);
            available_columns.extend(produced);

            return if let Some(parent) = parent_info {
                parent
            } else {
                UnnestingInfo::new(HashSet::new(), None)
            };
        }

        _ => {
            let mut children_infos = Vec::new();
            let mut children_available = Vec::new();

            for child in node.children_mut() {
                let mut child_available = HashSet::new();
                let child_info =
                    process_node(child, tree, parent_info.clone(), &mut child_available);
                children_infos.push(child_info);
                children_available.push(child_available);
            }

            let mut result_info = if let Some(parent) = parent_info {
                parent
            } else if !children_infos.is_empty() {
                children_infos[0].clone()
            } else {
                UnnestingInfo::new(HashSet::new(), None)
            };

            for child_info in &children_infos[1..] {
                result_info.merge_equivalence_classes(child_info);
            }

            available_columns.clear();
            for child_available in children_available {
                available_columns.extend(child_available);
            }

            return result_info;
        }
    }
}

fn try_simple_unnesting(
    node: &mut RelNode,
    accessing: &HashSet<NodeId>,
    condition: &mut Expr,
) -> bool {
    let mut all_predicates = true;
    let mut predicates = Vec::new();

    for &acc_id in accessing {
        if let Some(acc_node) = find_node_by_id_mut(node, acc_id) {
            if let RelNode::Select { predicate, .. } = acc_node {
                predicates.push(predicate.clone());
            } else {
                all_predicates = false;
                break;
            }
        }
    }

    if all_predicates && !predicates.is_empty() {
        let mut new_condition = condition.clone();

        for pred in predicates {
            new_condition = Expr::And(Box::new(new_condition), Box::new(pred));
        }

        *condition = new_condition;
        return true;
    }

    false
}

fn find_correlation(left: &RelNode, right: &RelNode) -> (HashSet<Column>, HashSet<Column>) {
    let left_cols = get_node_produced_columns(left);
    let right_free_vars = get_node_free_variables(right);

    (left_cols, right_free_vars)
}

fn get_node_produced_columns(node: &RelNode) -> HashSet<Column> {
    match node {
        RelNode::Table { name, columns, .. } => columns
            .iter()
            .map(|col_name| Column::new(name, col_name))
            .collect(),
        RelNode::Map {
            mappings, input, ..
        } => {
            let mut result = get_node_produced_columns(input);
            for (col, _) in mappings {
                result.insert(col.clone());
            }
            result
        }
        RelNode::Select { input, .. } => get_node_produced_columns(input),
        RelNode::GroupBy {
            keys, aggregates, ..
        } => {
            let mut result = HashSet::new();
            result.extend(keys.iter().cloned());
            result.extend(aggregates.keys().cloned());
            result
        }
        RelNode::Join { left, right, .. } => {
            let mut result = get_node_produced_columns(left);
            result.extend(get_node_produced_columns(right));
            result
        }
        _ => HashSet::new(),
    }
}

fn get_node_free_variables(node: &RelNode) -> HashSet<Column> {
    let mut result = HashSet::new();

    match node {
        RelNode::Select {
            predicate, input, ..
        } => {
            let pred_cols = get_expr_columns(predicate);
            let input_cols = get_node_produced_columns(input);

            for col in pred_cols {
                if !input_cols.contains(&col) {
                    result.insert(col);
                }
            }

            result.extend(get_node_free_variables(input));
        }
        RelNode::Map {
            mappings, input, ..
        } => {
            let input_cols = get_node_produced_columns(input);

            for (_, expr) in mappings {
                let expr_cols = get_expr_columns(expr);
                for col in expr_cols {
                    if !input_cols.contains(&col) {
                        result.insert(col);
                    }
                }
            }

            result.extend(get_node_free_variables(input));
        }
        RelNode::GroupBy {
            aggregates, input, ..
        } => {
            let input_cols = get_node_produced_columns(input);

            for (_, expr) in aggregates {
                let expr_cols = get_expr_columns(expr);
                for col in expr_cols {
                    if !input_cols.contains(&col) {
                        result.insert(col);
                    }
                }
            }

            result.extend(get_node_free_variables(input));
        }
        RelNode::Join {
            condition,
            left,
            right,
            ..
        } => {
            let cond_cols = get_expr_columns(condition);
            let left_cols = get_node_produced_columns(left);
            let right_cols = get_node_produced_columns(right);

            for col in cond_cols {
                if !left_cols.contains(&col) && !right_cols.contains(&col) {
                    result.insert(col);
                }
            }

            result.extend(get_node_free_variables(left));
            result.extend(get_node_free_variables(right));
        }
        _ => {
            for child in node.children() {
                result.extend(get_node_free_variables(child));
            }
        }
    }

    result
}

fn get_expr_columns(expr: &Expr) -> HashSet<Column> {
    let mut result = HashSet::new();

    match expr {
        Expr::ColumnRef(col) => {
            result.insert(col.clone());
        }
        Expr::Equal(left, right) | Expr::And(left, right) | Expr::GreaterThan(left, right) => {
            result.extend(get_expr_columns(left));
            result.extend(get_expr_columns(right));
        }
        Expr::Sum(inner) => {
            result.extend(get_expr_columns(inner));
        }
        _ => {}
    }

    result
}

fn add_equivalences_from_expr(expr: &Expr, cclasses: &mut HashMap<Column, HashSet<Column>>) {
    match expr {
        Expr::Equal(left, right) => {
            if let (Expr::ColumnRef(left_col), Expr::ColumnRef(right_col)) = (&**left, &**right) {
                cclasses
                    .entry(left_col.clone())
                    .or_insert_with(HashSet::new)
                    .insert(right_col.clone());

                cclasses
                    .entry(right_col.clone())
                    .or_insert_with(HashSet::new)
                    .insert(left_col.clone());
            }
        }
        Expr::And(left, right) => {
            add_equivalences_from_expr(left, cclasses);
            add_equivalences_from_expr(right, cclasses);
        }
        _ => {}
    }
}

fn rewrite_expr(expr: &Expr, repr: &HashMap<Column, Column>) -> Expr {
    match expr {
        Expr::ColumnRef(col) => {
            if let Some(new_col) = repr.get(col) {
                Expr::ColumnRef(new_col.clone())
            } else {
                expr.clone()
            }
        }
        Expr::Equal(left, right) => Expr::Equal(
            Box::new(rewrite_expr(left, repr)),
            Box::new(rewrite_expr(right, repr)),
        ),
        Expr::And(left, right) => Expr::And(
            Box::new(rewrite_expr(left, repr)),
            Box::new(rewrite_expr(right, repr)),
        ),
        Expr::GreaterThan(left, right) => Expr::GreaterThan(
            Box::new(rewrite_expr(left, repr)),
            Box::new(rewrite_expr(right, repr)),
        ),
        Expr::Sum(inner) => Expr::Sum(Box::new(rewrite_expr(inner, repr))),
        _ => expr.clone(),
    }
}

fn create_domain_node(left: &RelNode, outer_refs: &HashSet<Column>) -> RelNode {
    let mut mappings = HashMap::new();
    for col in outer_refs {
        mappings.insert(col.clone(), Expr::ColumnRef(col.clone()));
    }

    RelNode::Map {
        id: get_next_id(),
        mappings,
        input: Box::new(left.clone()),
    }
}

fn create_domain_join_condition(
    outer_refs: &HashSet<Column>,
    repr: &HashMap<Column, Column>,
) -> Expr {
    let mut condition = Expr::Constant("true".to_string());

    for outer_col in outer_refs {
        if let Some(new_col) = repr.get(outer_col) {
            let eq_condition = Expr::Equal(
                Box::new(Expr::ColumnRef(outer_col.clone())),
                Box::new(Expr::ColumnRef(new_col.clone())),
            );

            condition = Expr::And(Box::new(condition), Box::new(eq_condition));
        }
    }

    condition
}

fn merge_unnesting_info(left: UnnestingInfo, right: UnnestingInfo) -> UnnestingInfo {
    let mut result = if !left.outer_refs.is_empty() {
        left.clone()
    } else {
        right.clone()
    };

    if !left.outer_refs.is_empty() && !right.outer_refs.is_empty() {
        for (col, equivalents) in &right.cclasses {
            let entry = result
                .cclasses
                .entry(col.clone())
                .or_insert_with(HashSet::new);
            entry.extend(equivalents.iter().cloned());
        }

        for (col, new_col) in &right.repr {
            if !result.repr.contains_key(col) {
                result.repr.insert(col.clone(), new_col.clone());
            }
        }
    }

    result
}

/// 基于 unnesting 信息 decorrelate 节点
fn decorrelate_node(node: &RelNode, info: &UnnestingInfo) -> RelNode {
    // 在实际实现中，这将递归转换整个子树
    // 这里简化为仅返回原节点的clone
    node.clone()
}

fn get_next_id() -> NodeId {
    static mut NEXT_ID: NodeId = 1000;

    unsafe {
        NEXT_ID += 1;
        NEXT_ID
    }
}

fn find_node_by_id_mut<'a>(node: &'a mut RelNode, id: NodeId) -> Option<&'a mut RelNode> {
    if node.id() == id {
        return Some(node);
    }

    for child in node.children_mut() {
        if let Some(result) = find_node_by_id_mut(child, id) {
            return Some(result);
        }
    }

    None
}
