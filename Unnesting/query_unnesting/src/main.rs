mod query_tree;
mod unnesting;

use crate::query_tree::QueryTree;
use crate::unnesting::{process_node, unnest_query};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};

type NodeId = usize;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    pub table: String,
    pub name: String,
}

impl Column {
    pub fn new(table: &str, name: &str) -> Self {
        Self {
            table: table.to_string(),
            name: name.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}.{}", self.table, self.name)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Expr {
    ColumnRef(Column),
    Constant(String),
    Equal(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    GreaterThan(Box<Expr>, Box<Expr>),
    Count,
    Sum(Box<Expr>),
}

impl Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::ColumnRef(c) => write!(f, "{}", c.to_string()),
            Expr::Constant(v) => write!(f, "{}", v),
            Expr::Equal(l, r) => write!(f, "{:?} = {:?}", l, r),
            Expr::And(l, r) => write!(f, "({:?} AND {:?})", l, r),
            Expr::GreaterThan(l, r) => write!(f, "{:?} > {:?}", l, r),
            Expr::Count => write!(f, "COUNT(*)"),
            Expr::Sum(e) => write!(f, "SUM({:?})", e),
        }
    }
}

#[derive(Clone)]
pub enum RelNode {
    Table {
        id: NodeId,
        name: String,
        columns: Vec<String>,
    },

    Select {
        id: NodeId,
        predicate: Expr,
        input: Box<RelNode>,
    },

    Map {
        id: NodeId,
        mappings: HashMap<Column, Expr>,
        input: Box<RelNode>,
    },

    GroupBy {
        id: NodeId,
        keys: Vec<Column>,
        aggregates: HashMap<Column, Expr>,
        input: Box<RelNode>,
    },

    Join {
        id: NodeId,
        left: Box<RelNode>,
        right: Box<RelNode>,
        condition: Expr,
        is_dependent: bool,
        accessing: HashSet<NodeId>,
    },
}

impl Debug for RelNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelNode::Table { id, name, .. } => write!(f, "Table[{}]({})", id, name),
            RelNode::Select {
                id,
                predicate,
                input,
            } => write!(f, "Select[{}]({:?}, {:?})", id, predicate, input),
            RelNode::Map { id, input, .. } => write!(f, "Map[{}]({:?})", id, input),
            RelNode::GroupBy {
                id, keys, input, ..
            } => write!(f, "GroupBy[{}]({:?}, {:?})", id, keys, input),
            RelNode::Join {
                id,
                left,
                right,
                is_dependent,
                ..
            } => {
                let join_type = if *is_dependent {
                    "DependentJoin"
                } else {
                    "Join"
                };
                write!(f, "{}[{}]({:?}, {:?})", join_type, id, left, right)
            }
        }
    }
}

impl RelNode {
    pub fn id(&self) -> NodeId {
        match self {
            RelNode::Table { id, .. } => *id,
            RelNode::Select { id, .. } => *id,
            RelNode::Map { id, .. } => *id,
            RelNode::GroupBy { id, .. } => *id,
            RelNode::Join { id, .. } => *id,
        }
    }

    pub fn children(&self) -> Vec<&RelNode> {
        match self {
            RelNode::Table { .. } => vec![],
            RelNode::Select { input, .. } => vec![input],
            RelNode::Map { input, .. } => vec![input],
            RelNode::GroupBy { input, .. } => vec![input],
            RelNode::Join { left, right, .. } => vec![left, right],
        }
    }

    pub fn children_mut(&mut self) -> Vec<&mut RelNode> {
        match self {
            RelNode::Table { .. } => vec![],
            RelNode::Select { input, .. } => vec![input],
            RelNode::Map { input, .. } => vec![input],
            RelNode::GroupBy { input, .. } => vec![input],
            RelNode::Join { left, right, .. } => vec![left, right],
        }
    }

    pub fn get_accessed_columns(&self) -> HashSet<Column> {
        let mut columns = HashSet::new();

        match self {
            RelNode::Table { .. } => {}
            RelNode::Select { predicate, .. } => {
                collect_columns_from_expr(predicate, &mut columns);
            }
            RelNode::Map { mappings, .. } => {
                for expr in mappings.values() {
                    collect_columns_from_expr(expr, &mut columns);
                }
            }
            RelNode::GroupBy {
                keys, aggregates, ..
            } => {
                columns.extend(keys.clone());
                for expr in aggregates.values() {
                    collect_columns_from_expr(expr, &mut columns);
                }
            }
            RelNode::Join { condition, .. } => {
                collect_columns_from_expr(condition, &mut columns);
            }
        }

        columns
    }

    pub fn get_produced_columns(&self) -> HashSet<Column> {
        let mut columns = HashSet::new();

        match self {
            RelNode::Table {
                name,
                columns: cols,
                ..
            } => {
                for col in cols {
                    columns.insert(Column::new(name, col));
                }
            }
            RelNode::Select { input, .. } => {
                columns.extend(input.get_produced_columns());
            }
            RelNode::Map {
                mappings, input, ..
            } => {
                columns.extend(input.get_produced_columns());
                columns.extend(mappings.keys().cloned());
            }
            RelNode::GroupBy {
                keys, aggregates, ..
            } => {
                columns.extend(keys.clone());
                columns.extend(aggregates.keys().cloned());
            }
            RelNode::Join { left, right, .. } => {
                columns.extend(left.get_produced_columns());
                columns.extend(right.get_produced_columns());
            }
        }

        columns
    }

    pub fn left(&self) -> &RelNode {
        match self {
            RelNode::Join { left, .. } => left,
            _ => panic!("Not a join node"),
        }
    }

    pub fn right(&self) -> &RelNode {
        match self {
            RelNode::Join { right, .. } => right,
            _ => panic!("Not a join node"),
        }
    }

    pub fn accessing_mut(&mut self) -> &mut HashSet<NodeId> {
        match self {
            RelNode::Join { accessing, .. } => accessing,
            _ => panic!("Not a join node"),
        }
    }

    pub fn is_dependent_join(&self) -> bool {
        match self {
            RelNode::Join { is_dependent, .. } => *is_dependent,
            _ => false,
        }
    }
}

fn collect_columns_from_expr(expr: &Expr, columns: &mut HashSet<Column>) {
    match expr {
        Expr::ColumnRef(col) => {
            columns.insert(col.clone());
        }
        Expr::Constant(_) | Expr::Count => {}
        Expr::Equal(left, right) | Expr::And(left, right) | Expr::GreaterThan(left, right) => {
            collect_columns_from_expr(left, columns);
            collect_columns_from_expr(right, columns);
        }
        Expr::Sum(e) => collect_columns_from_expr(e, columns),
    }
}

fn build_column_lineage(
    node: &RelNode,
    providers: &mut HashMap<Column, NodeId>,
    parent_map: &mut HashMap<NodeId, NodeId>,
    parent: Option<NodeId>,
) {
    if let Some(p) = parent {
        parent_map.insert(node.id(), p);
    }

    match node {
        RelNode::Table { id, name, columns } => {
            for col in columns {
                providers.insert(Column::new(name, col), *id);
            }
        }

        RelNode::Join {
            id, left, right, ..
        } => {
            build_column_lineage(left, providers, parent_map, Some(*id));
            build_column_lineage(right, providers, parent_map, Some(*id));
        }

        RelNode::Map {
            id,
            input,
            mappings,
        } => {
            build_column_lineage(input, providers, parent_map, Some(*id));

            for (new_col, _) in mappings {
                providers.insert(new_col.clone(), *id);
            }
        }

        _ => {
            for child in node.children() {
                build_column_lineage(child, providers, parent_map, Some(node.id()));
            }
        }
    }
}

pub fn unnest_query(query: RelNode) -> RelNode {
    let tree = QueryTree::new(query);

    let relations = tree.identify_dependent_joins();

    tree.apply_dependencies(&relations);

    let mut root_node = tree.root.borrow().clone();

    let mut available_columns = HashSet::new();
    process_node(&mut root_node, &tree, None, &mut available_columns);

    root_node
}

fn main() {
    //let query = build_example_query();

    let unnested = unnest_query(query);

    println!("Unnested query plan: {:?}", unnested);
}
