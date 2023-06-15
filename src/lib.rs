#![no_std]

use hashbrown::HashMap;
use hashbrown::HashSet;
use core::hash::Hash;

#[derive(Clone, Debug)]
pub struct Table<'a, C: Eq + Hash, R: Eq + Hash, V: Eq + Hash> {
    tuples: HashMap<(&'a C, &'a R), HashSet<&'a V>>,
    columns: HashMap<&'a C, HashSet<&'a V>>,
    rows: HashMap<&'a R, HashSet<&'a V>>,
}

impl<'a, C: Eq + Hash, R: Eq + Hash, V: Eq + Hash> Table<'a, C, R, V> {
    pub fn new() -> Table<'a, C, R, V> {
        Table {
            tuples: HashMap::new(),
            columns: HashMap::new(),
            rows: HashMap::new(),
        }
    }

    pub fn insert(&mut self, column: &'a C, row: &'a R, value: &'a V) -> &mut Self {
        self.tuples.entry((column, row)).or_insert_with(HashSet::new).insert(value);
        self.columns.entry(column).or_insert_with(HashSet::new).insert(value);
        self.rows.entry(row).or_insert_with(HashSet::new).insert(value);
        self
    }

    pub fn remove(&mut self, column: &'a C, row: &'a R, value: &'a V) -> &mut Self {
        remove_from_set_and_map(&mut self.tuples, &(column, row), value);
        remove_from_set_and_map(&mut self.columns, &column, value);
        remove_from_set_and_map(&mut self.rows, &row, value);
        self
    }

    pub fn remove_by_value(&mut self, value_to_remove: &'a V) -> &mut Self {
        let mut items_to_remove = HashSet::<(&C, &R)>::new();

        for (&tuple, values) in self.tuples.iter() {
            if values.contains(value_to_remove) {
                items_to_remove.insert(tuple);
            }
        }

        for (column, row_to_remove) in items_to_remove {
            self.remove(column, row_to_remove, value_to_remove);
        }

        self
    }

    pub fn remove_by_row(&mut self, row_to_remove: &'a R) -> &mut Self {
        let mut items_to_remove = HashSet::<(&C, &V)>::new();

        for (tuple, vals) in self.tuples.iter() {
            if tuple.1 == row_to_remove {
                for &value in vals {
                    let item = (tuple.0, value);
                    items_to_remove.insert(item);
                }
            }
        }

        for (column, value) in items_to_remove {
            self.remove(column, row_to_remove, value);
        }

        self
    }

    pub fn remove_by_column(&mut self, column_to_remove: &'a C) -> &mut Self {
        let mut items_to_remove = HashSet::<(&R, &V)>::new();

        for (tuple, vals) in self.tuples.iter() {
            if tuple.0 == column_to_remove {
                for &value in vals {
                    let item = (tuple.1, value);
                    items_to_remove.insert(item);
                }
            }
        }

        for (row, value) in items_to_remove {
            self.remove(column_to_remove, row, value);
        }

        self
    }

    pub fn is_empty(&self) -> bool {
        let all_empty = self.tuples.is_empty() && self.columns.is_empty() && self.rows.is_empty();
        let all_non_empty = !self.tuples.is_empty() && !self.columns.is_empty() && !self.rows.is_empty();
        assert!(all_empty || all_non_empty);
        all_empty
    }
}

fn remove_from_set_and_map<K: Eq + Hash, V: Eq + Hash>(map: &mut HashMap<K, HashSet<&V>>, key: &K, value: &V) {
    if let Some(inner_set) = map.get_mut(key) {
        inner_set.remove(value);

        if inner_set.is_empty() {
            map.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_ops() {
        let mut table: Table<&str, &str, i32> = Table::new();
        table.insert(&"a", &"b", &11);
        table.insert(&"a", &"b", &12);
        table.insert(&"a", &"c", &12);
        table.insert(&"d", &"b", &10);

        assert_eq!(table.tuples.get(&(&"a", &"b")).unwrap().len(), 2);
        assert_eq!(table.tuples.get(&(&"a", &"c")).unwrap().len(), 1);
        assert_eq!(table.tuples.get(&(&"d", &"b")).unwrap().len(), 1);
        assert!(table.remove_by_value(&10).remove_by_value(&11).remove_by_value(&12).is_empty());
    }

    #[test]
    fn remove_handle_duplicate() {
        let mut table: Table<&str, &str, i32> = Table::new();
        table.insert(&"a", &"b", &12);
        table.insert(&"a", &"b", &12);
        assert!(!table.remove(&"a", &"c", &13).is_empty());
        assert!(table.remove(&"a", &"b", &12).is_empty());
    }

    #[test]
    fn remove_by_value() {
        let mut table: Table<&str, &str, i32> = Table::new();
        table.insert(&"a", &"b", &12);
        table.insert(&"a", &"b", &14);
        assert_eq!(table.remove_by_value(&11).tuples.get(&(&"a", &"b")).unwrap().len(), 2);
        assert!(!table.remove_by_value(&12).is_empty());
        assert_eq!(table.remove_by_value(&11).tuples.get(&(&"a", &"b")).unwrap().len(), 1);
        assert!(table.remove_by_value(&14).is_empty());
    }

    #[test]
    fn remove_by_column() {
        let mut table: Table<&str, &str, i32> = Table::new();
        table.insert(&"a", &"b", &12);
        table.insert(&"a", &"c", &14);
        assert!(table.remove_by_column(&"a").is_empty());
    }

    #[test]
    fn remove_by_row() {
        let mut table: Table<&str, &str, i32> = Table::new();
        table.insert(&"b", &"a", &12);
        table.insert(&"c", &"a", &14);
        assert!(table.remove_by_row(&"a").is_empty());
    }
}