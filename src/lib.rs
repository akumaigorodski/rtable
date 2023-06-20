#![no_std]

extern crate alloc;

use alloc::string::String;
use hashbrown::HashMap;
use hashbrown::HashSet;
use core::hash::Hash;

pub trait TableKV: Eq + PartialEq {
    fn id(&self) -> String;
}

#[derive(Clone, Debug)]
pub struct Table<C: TableKV, R: TableKV, V: TableKV> {
    tuples: HashMap<(String, String), HashSet<String>>,
    columns: HashMap<String, HashSet<String>>,
    rows: HashMap<String, HashSet<String>>,
    vs: HashMap<String, V>,
    cs: HashMap<String, C>,
    rs: HashMap<String, R>,
}

impl<C: TableKV, R: TableKV, V: TableKV> Table<C, R, V> {
    pub fn new() -> Self {
        Table {
            tuples: HashMap::new(),
            columns: HashMap::new(),
            rows: HashMap::new(),
            vs: HashMap::new(),
            cs: HashMap::new(),
            rs: HashMap::new(),
        }
    }

    pub fn insert(&mut self, column: C, row: R, value: V) -> &mut Self {
        let column_key = column.id();
        let value_key = value.id();
        let row_key = row.id();

        let column_key_clone = column_key.clone();
        let row_key_clone = row_key.clone();

        let column_row_key = (column_key_clone, row_key_clone);
        self.tuples.entry(column_row_key).or_insert_with(HashSet::new).insert(value_key.clone());
        self.columns.entry(column_key.clone()).or_insert_with(HashSet::new).insert(value_key.clone());
        self.rows.entry(row_key.clone()).or_insert_with(HashSet::new).insert(value_key.clone());

        self.vs.insert(value_key, value);
        self.cs.insert(column_key, column);
        self.rs.insert(row_key, row);

        self
    }

    pub fn remove(&mut self, column_key: &String, row_key: &String, value_key: &String) -> &mut Self {
        let column_key_clone = column_key.clone();
        let row_key_clone = row_key.clone();

        let column_row_key = &(column_key_clone, row_key_clone);
        remove_from_set_and_map(&mut self.tuples, column_row_key, value_key);
        remove_from_set_and_map(&mut self.columns, column_key, value_key);
        remove_from_set_and_map(&mut self.rows, row_key, value_key);

        self.vs.remove(value_key);

        if !self.columns.contains_key(column_key) {
            self.cs.remove(column_key);
        }

        if !self.rows.contains_key(row_key) {
            self.rs.remove(row_key);
        }

        self
    }

    pub fn remove_by_row(&mut self, row_key_to_remove: &String) -> &mut Self {
        let mut items_to_remove = HashSet::<(String, String)>::new();

        for (tuple, vals) in &self.tuples {
            if tuple.1 == *row_key_to_remove {
                for value_key in vals {
                    let column_key = tuple.0.clone();
                    let value_key = value_key.clone();
                    let item = (column_key, value_key);
                    items_to_remove.insert(item);
                }
            }
        }

        for (column_key, value_key) in items_to_remove {
            self.remove(&column_key, row_key_to_remove, &value_key);
        }

        self
    }

    pub fn remove_by_column(&mut self, column_key_to_remove: &String) -> &mut Self {
        let mut items_to_remove = HashSet::<(String, String)>::new();

        for (tuple, vals) in &self.tuples {
            if tuple.0 == *column_key_to_remove {
                for value_key in vals {
                    let row_key = tuple.1.clone();
                    let value_key = value_key.clone();
                    let item = (row_key, value_key);
                    items_to_remove.insert(item);
                }
            }
        }

        for (row_key, value_key) in items_to_remove {
            self.remove(column_key_to_remove, &row_key, &value_key);
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

#[derive(Clone, Debug)]
pub struct InverseTable {
    // This column has these values except those at intersection with this row
    column_value_keys_except: HashMap<(String, String), HashSet<String>>,
    // This row has these values except those at intersection with this column
    row_value_keys_except: HashMap<(String, String), HashSet<String>>,
}

impl InverseTable {
    pub fn rebuild_from<C: TableKV, R: TableKV, V: TableKV>(table: &Table<C, R, V>) -> Self {
        let mut column_value_keys_except = HashMap::<(String, String), HashSet<String>>::new();
        let mut row_value_keys_except = HashMap::<(String, String), HashSet<String>>::new();

        for key @ (column_key, row_key) in table.tuples.keys() {
            let column_value_keys = table.columns.get(column_key).unwrap();
            let row_value_keys = table.rows.get(row_key).unwrap();

            let column_values_diff = column_value_keys.difference(row_value_keys).cloned().collect();
            let row_values_diff = row_value_keys.difference(column_value_keys).cloned().collect();

            column_value_keys_except.insert(key.clone(), column_values_diff);
            row_value_keys_except.insert(key.clone(), row_values_diff);
        }

        InverseTable {
            column_value_keys_except,
            row_value_keys_except
        }
    }
}

// A utility to remove a value from HashSet which is a value of HashMap, and then remove a HashMap key if set becomes empty
pub fn remove_from_set_and_map<K: Eq + Hash, V: Eq + Hash>(map: &mut HashMap<K, HashSet<V>>, key: &K, value: &V) {
    if let Some(inner_set) = map.get_mut(key) {
        inner_set.remove(value);

        if inner_set.is_empty() {
            map.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;
    use super::*;

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct Container(String);

    impl TableKV for Container {
        fn id(&self) -> String {
            self.0.clone()
        }
    }

    #[test]
    fn basic_ops() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("1".to_string()), Container("2".to_string()), Container("11".to_string()));
        table.insert(Container("1".to_string()), Container("2".to_string()), Container("11".to_string()));
        table.insert(Container("1".to_string()), Container("2".to_string()), Container("12".to_string()));
        table.insert(Container("1".to_string()), Container("3".to_string()), Container("12".to_string()));
        table.insert(Container("4".to_string()), Container("2".to_string()), Container("10".to_string()));

        assert_eq!(table.tuples.get(&("1".to_string(), "2".to_string())).unwrap().len(), 2);
        assert_eq!(table.tuples.get(&("1".to_string(), "3".to_string())).unwrap().len(), 1);
        assert_eq!(table.tuples.get(&("4".to_string(), "2".to_string())).unwrap().len(), 1);
        assert!(table.remove_by_column(&"4".to_string()).remove_by_column(&"1".to_string()).is_empty());
    }

    #[test]
    fn remove_handle_duplicate() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a".to_string()), Container("b".to_string()), Container("11".to_string()));
        table.insert(Container("a".to_string()), Container("b".to_string()), Container("11".to_string()));
        assert!(!table.remove(&"a".to_string(), &"c".to_string(), &"12".to_string()).is_empty());
        assert!(table.remove(&"a".to_string(), &"b".to_string(), &"11".to_string()).is_empty());
    }

    #[test]
    fn remove_by_column() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a".to_string()), Container("b".to_string()), Container("11".to_string()));
        table.insert(Container("a".to_string()), Container("b".to_string()), Container("12".to_string()));
        table.insert(Container("a".to_string()), Container("c".to_string()), Container("12".to_string()));
        table.insert(Container("a".to_string()), Container("c".to_string()), Container("14".to_string()));
        assert!(table.remove_by_column(&"a".to_string()).is_empty());
    }

    #[test]
    fn remove_by_row() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("c".to_string()), Container("a".to_string()), Container("11".to_string()));
        table.insert(Container("b".to_string()), Container("a".to_string()), Container("12".to_string()));
        table.insert(Container("d".to_string()), Container("a".to_string()), Container("12".to_string()));
        table.insert(Container("e".to_string()), Container("a".to_string()), Container("14".to_string()));
        assert!(table.remove_by_row(&"a".to_string()).is_empty());
    }

    #[test]
    fn inverse_table() {
        let mut hs1 = HashSet::<String>::new();
        hs1.insert("12".to_string());
        hs1.insert("15".to_string());

        let mut hs2 = HashSet::<String>::new();
        hs2.insert("16".to_string());

        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a".to_string()), Container("b".to_string()), Container("12".to_string()));
        table.insert(Container("a".to_string()), Container("c".to_string()), Container("14".to_string()));
        table.insert(Container("a".to_string()), Container("d".to_string()), Container("15".to_string()));
        table.insert(Container("e".to_string()), Container("b".to_string()), Container("16".to_string()));

        let inverse: InverseTable = InverseTable::rebuild_from(&table);
        assert_eq!(inverse.column_value_keys_except.get(&("a".to_string(), "c".to_string())).unwrap(), &hs1);
        assert_eq!(inverse.row_value_keys_except.get(&("a".to_string(), "b".to_string())).unwrap(), &hs2);
    }
}