#![no_std]

use hashbrown::HashMap;
use hashbrown::HashSet;
use core::hash::Hash;

pub trait TableKV<'a>: Eq + PartialEq + Hash {
    fn id(&self) -> &'a str;
}

#[derive(Clone, Debug)]
pub struct Table<'a, C: TableKV<'a>, R: TableKV<'a>, V: TableKV<'a>> {
    tuples: HashMap<(&'a str, &'a str), HashSet<&'a str>>, // This intersection has these values
    columns: HashMap<&'a str, HashSet<&'a str>>, // This column has these values
    rows: HashMap<&'a str, HashSet<&'a str>>, // This row has these values
    cs: HashMap<&'a str, C>,
    rs: HashMap<&'a str, R>,
    vs: HashMap<&'a str, V>,
}

impl<'a, C: TableKV<'a>, R: TableKV<'a>, V: TableKV<'a>> Table<'a, C, R, V> {
    pub fn new() -> Self {
        Table {
            tuples: HashMap::new(),
            columns: HashMap::new(),
            rows: HashMap::new(),
            cs: HashMap::new(),
            rs: HashMap::new(),
            vs: HashMap::new(),
        }
    }

    pub fn insert(&mut self, column: C, row: R, value: V) -> &mut Self {
        let column_key = column.id();
        let value_key = value.id();
        let row_key = row.id();

        let column_row_key = (column_key, row_key);
        self.rows.entry(row_key).or_insert_with(HashSet::new).insert(value_key);
        self.columns.entry(column_key).or_insert_with(HashSet::new).insert(value_key);
        self.tuples.entry(column_row_key).or_insert_with(HashSet::new).insert(value_key);

        self.vs.insert(value_key, value);
        self.cs.insert(column_key, column);
        self.rs.insert(row_key, row);

        self
    }

    pub fn remove(&mut self, column_key: &'a str, row_key: &'a str, value_key: &'a str) -> &mut Self {
        remove_from_set_and_map(&mut self.tuples, (column_key, row_key), value_key);
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

    pub fn remove_by_row(&mut self, row_key_to_remove: &'a str) -> &mut Self {
        let mut items_to_remove = HashSet::<(&'a str, &'a str)>::new();

        for (tuple, vals) in &self.tuples {
            if tuple.1 == row_key_to_remove {
                for value_key in vals {
                    let item = (tuple.0, *value_key);
                    items_to_remove.insert(item);
                }
            }
        }

        for (column_key, value_key) in items_to_remove {
            self.remove(column_key, row_key_to_remove, value_key);
        }

        self
    }

    pub fn remove_by_column(&mut self, column_key_to_remove: &'a str) -> &mut Self {
        let mut items_to_remove = HashSet::<(&'a str, &'a str)>::new();

        for (tuple, vals) in &self.tuples {
            if tuple.0 == column_key_to_remove {
                for value_key in vals {
                    let item = (tuple.1, *value_key);
                    items_to_remove.insert(item);
                }
            }
        }

        for (row_key, value_key) in items_to_remove {
            self.remove(column_key_to_remove, row_key, value_key);
        }

        self
    }

    pub fn is_empty(&self) -> bool {
        let all_empty = self.tuples.is_empty() && self.columns.is_empty() && self.rows.is_empty();
        let all_non_empty = !self.tuples.is_empty() && !self.columns.is_empty() && !self.rows.is_empty();
        assert!(all_empty || all_non_empty);
        all_empty
    }

    pub fn extract_values<T: TableKV<'a>>(&self, keys: &'a HashSet<&str>, storage: &'a HashMap<&str, T>) -> HashSet<&'a T> {
        keys.iter().filter_map(|key| storage.get(key)).collect()
    }
}

#[derive(Clone, Debug)]
pub struct InverseTable<'a> {
    column_value_keys_except: HashMap<(&'a str, &'a str), HashSet<&'a str>>, // This column has these values except those at intersection with this row
    row_value_keys_except: HashMap<(&'a str, &'a str), HashSet<&'a str>>, // This row has these values except those at intersection with this column
}

impl<'a> InverseTable<'a> {
    pub fn rebuild_from<C: TableKV<'a>, R: TableKV<'a>, V: TableKV<'a>>(table: &Table<'a, C, R, V>) -> Self {
        let mut column_value_keys_except = HashMap::<(&'a str, &'a str), HashSet<&'a str>>::new();
        let mut row_value_keys_except = HashMap::<(&'a str, &'a str), HashSet<&'a str>>:: new();

        for key @ (column_key, row_key) in table.tuples.keys() {
            let column_value_keys = table.columns.get(column_key).unwrap();
            let row_value_keys = table.rows.get(row_key).unwrap();

            let column_values_diff = column_value_keys.difference(row_value_keys).map(|&value| value).collect();
            let row_values_diff = row_value_keys.difference(column_value_keys).map(|&value| value).collect();

            column_value_keys_except.insert(*key, column_values_diff);
            row_value_keys_except.insert(*key, row_values_diff);
        }

        InverseTable {
            column_value_keys_except,
            row_value_keys_except
        }
    }
}

fn remove_from_set_and_map<K: Eq + Hash>(map: &mut HashMap<K, HashSet<&str>>, key: K, value: &str) {
    if let Some(inner_set) = map.get_mut(&key) {
        inner_set.remove(value);

        if inner_set.is_empty() {
            map.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct Container<'a>(&'a str);

    impl<'a> TableKV<'a> for Container<'a> {
        fn id(&self) -> &'a str {
            self.0
        }
    }

    #[test]
    fn basic_ops() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a"), Container("b"), Container("11"));
        table.insert(Container("a"), Container("b"), Container("12"));
        table.insert(Container("a"), Container("c"), Container("12"));
        table.insert(Container("d"), Container("b"), Container("10"));

        assert_eq!(table.tuples.get(&("a", "b")).unwrap().len(), 2);
        assert_eq!(table.tuples.get(&("a", "c")).unwrap().len(), 1);
        assert_eq!(table.tuples.get(&("d", "b")).unwrap().len(), 1);
    }

    #[test]
    fn remove_handle_duplicate() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a"), Container("b"), Container("11"));
        table.insert(Container("a"), Container("b"), Container("11"));
        assert!(!table.remove("a", "c", "12").is_empty());
        assert!(table.remove("a", "b", "11").is_empty());
    }

    #[test]
    fn remove_by_column() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a"), Container("b"), Container("11"));
        table.insert(Container("a"), Container("b"), Container("12"));
        table.insert(Container("a"), Container("c"), Container("12"));
        table.insert(Container("a"), Container("c"), Container("14"));
        assert!(table.remove_by_column("a").is_empty());
    }

    #[test]
    fn remove_by_row() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("c"), Container("a"), Container("11"));
        table.insert(Container("b"), Container("a"), Container("12"));
        table.insert(Container("d"), Container("a"), Container("12"));
        table.insert(Container("e"), Container("a"), Container("14"));
        assert!(table.remove_by_row("a").is_empty());
    }

    #[test]
    fn inverse_table() {
        let mut hs1 = HashSet::<&str>::new();
        hs1.insert(&"12");
        hs1.insert(&"15");

        let mut hs2 = HashSet::<&str>::new();
        hs2.insert(&"16");

        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a"), Container("b"), Container("12"));
        table.insert(Container("a"), Container("c"), Container("14"));
        table.insert(Container("a"), Container("d"), Container("15"));
        table.insert(Container("e"), Container("b"), Container("16"));

        let inverse: InverseTable = InverseTable::rebuild_from(&table);
        assert_eq!(inverse.column_value_keys_except.get(&("a", "c")).unwrap(), &hs1);
        assert_eq!(inverse.row_value_keys_except.get(&("a", "b")).unwrap(), &hs2);
    }

    #[test]
    fn extract_values() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container("a"), Container("b"), Container("12"));
        table.insert(Container("a"), Container("c"), Container("14"));
        table.insert(Container("a"), Container("d"), Container("15"));
        table.insert(Container("e"), Container("b"), Container("16"));

        let mut hs1 = HashSet::<&str>::new();
        hs1.insert(&"14");
        hs1.insert(&"15");

        let inverse: InverseTable = InverseTable::rebuild_from(&table);
        let column_keys = inverse.column_value_keys_except.get(&("a", "b")).unwrap();
        let set: HashSet<&str> = table.extract_values(column_keys, &table.vs).iter().map(|v| v.0).collect();
        assert_eq!(set, hs1);
    }
}