use crate::{Column, NodeId, RelNode};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

type NodeRef = Rc<RefCell<RelNode>>;

pub struct QueryTree {
    pub(crate) root: NodeRef,
    node_map: HashMap<NodeId, NodeRef>,
    column_providers: HashMap<Column, NodeId>,
    parent_map: HashMap<NodeId, NodeId>,
}

impl QueryTree {
    pub fn new(root: RelNode) -> Self {
        let root_ref = Rc::new(RefCell::new(root));
        let mut tree = Self {
            root: Rc::clone(&root_ref),
            node_map: HashMap::new(),
            column_providers: HashMap::new(),
            parent_map: HashMap::new(),
        };

        tree.build_maps(Rc::clone(&root_ref), None);
        tree
    }

    fn build_maps(&mut self, node: NodeRef, parent: Option<NodeId>) {
        let id = node.borrow().id();

        self.node_map.insert(id, Rc::clone(&node));

        if let Some(parent_id) = parent {
            self.parent_map.insert(id, parent_id);
        }

        self.collect_column_providers(&node);

        let children: Vec<NodeRef> = {
            let node_borrow = node.borrow();
            node_borrow
                .children()
                .iter()
                .map(|child| {
                    let child_node = (*child).clone();
                    Rc::new(RefCell::new(child_node))
                })
                .collect()
        };

        for child in children {
            self.build_maps(Rc::clone(&child), Some(id));
        }
    }

    fn collect_column_providers(&mut self, node: &NodeRef) {
        let node_borrow = node.borrow();
        let id = node_borrow.id();

        match &*node_borrow {
            RelNode::Table { name, columns, .. } => {
                for col_name in columns {
                    let col = Column::new(name, col_name);
                    self.column_providers.insert(col, id);
                }
            }
            RelNode::Map { mappings, .. } => {
                for (col, _) in mappings {
                    self.column_providers.insert(col.clone(), id);
                }
            }
            RelNode::GroupBy {
                keys, aggregates, ..
            } => {
                for key in keys {
                    self.column_providers.insert(key.clone(), id);
                }
                for (col, _) in aggregates {
                    self.column_providers.insert(col.clone(), id);
                }
            }
            _ => {}
        }
    }

    pub fn root(&self) -> NodeRef {
        Rc::clone(&self.root)
    }

    pub fn find_node(&self, id: NodeId) -> Option<NodeRef> {
        self.node_map.get(&id).map(Rc::clone)
    }

    pub fn find_lca(&self, node1_id: NodeId, node2_id: NodeId) -> Option<NodeId> {
        let mut ancestors = HashSet::new();
        let mut current = node1_id;

        ancestors.insert(current);
        while let Some(&parent_id) = self.parent_map.get(&current) {
            ancestors.insert(parent_id);
            current = parent_id;
        }

        current = node2_id;
        if ancestors.contains(&current) {
            return Some(current);
        }

        while let Some(&parent_id) = self.parent_map.get(&current) {
            if ancestors.contains(&parent_id) {
                return Some(parent_id);
            }
            current = parent_id;
        }

        None
    }

    pub fn is_in_left_subtree(&self, node_id: NodeId, join_id: NodeId) -> bool {
        if let Some(join_node) = self.find_node(join_id) {
            let join_borrow = join_node.borrow();
            if let RelNode::Join { left, .. } = &*join_borrow {
                let left_id = left.id();
                return self.is_descendant_of(node_id, left_id);
            }
        }
        false
    }

    pub fn is_descendant_of(&self, node_id: NodeId, ancestor_id: NodeId) -> bool {
        if node_id == ancestor_id {
            return true;
        }

        let mut current = node_id;
        while let Some(&parent_id) = self.parent_map.get(&current) {
            if parent_id == ancestor_id {
                return true;
            }
            current = parent_id;
        }

        false
    }

    pub fn identify_dependent_joins(&self) -> Vec<(NodeId, NodeId)> {
        let mut relations = Vec::new();

        for (&node_id, node_ref) in &self.node_map {
            let accessed_columns = {
                let node_borrow = node_ref.borrow();
                node_borrow.get_accessed_columns()
            };

            for col in accessed_columns {
                if let Some(&provider_id) = self.column_providers.get(&col) {
                    if provider_id != node_id {
                        if let Some(lca_id) = self.find_lca(node_id, provider_id) {
                            if self.is_in_left_subtree(provider_id, lca_id) {
                                relations.push((lca_id, node_id));
                            }
                        }
                    }
                }
            }
        }

        relations
    }

    pub fn apply_dependencies(&self, relations: &[(NodeId, NodeId)]) {
        let mut lca_map: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        for &(lca_id, node_id) in relations {
            lca_map.entry(lca_id).or_default().push(node_id);
        }

        for (lca_id, accessing_ids) in lca_map {
            if let Some(lca_node) = self.find_node(lca_id) {
                let mut lca_borrow = lca_node.borrow_mut();
                if let RelNode::Join {
                    is_dependent,
                    accessing,
                    ..
                } = &mut *lca_borrow
                {
                    if *is_dependent {
                        for &id in &accessing_ids {
                            accessing.insert(id);
                        }
                    }
                }
            }
        }
    }
}
