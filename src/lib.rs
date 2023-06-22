//#![no_std]

use hashbrown::HashMap;
use hashbrown::HashSet;
use core::hash::Hash;

pub trait TableKV: Eq + PartialEq + Hash {
    fn id(&self) -> usize;
}

#[derive(Clone, Debug)]
pub struct Table<C: TableKV, R: TableKV, V: TableKV> {
    pub tuples: HashMap<(usize, usize), HashSet<usize>>,
    pub columns: HashMap<usize, HashSet<usize>>,
    pub rows: HashMap<usize, HashSet<usize>>,
    pub vs: HashMap<usize, V>,
    pub cs: HashMap<usize, C>,
    pub rs: HashMap<usize, R>,
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

    pub fn insert(&mut self, column: C, row: R, value: V) {
        let column_key = column.id();
        let value_key = value.id();
        let row_key = row.id();

        let column_row_key = (column_key, row_key);
        self.tuples.entry(column_row_key).or_insert_with(HashSet::new).insert(value_key);
        self.columns.entry(column_key).or_insert_with(HashSet::new).insert(value_key);
        self.rows.entry(row_key).or_insert_with(HashSet::new).insert(value_key);

        self.vs.insert(value_key, value);
        self.cs.insert(column_key, column);
        self.rs.insert(row_key, row);
    }

    pub fn remove(&mut self, column_key: usize, row_key: usize, value_key: usize) {
        remove_from_set_and_map(&mut self.tuples, &(column_key, row_key), &value_key);
        remove_from_set_and_map(&mut self.columns, &column_key, &value_key);
        remove_from_set_and_map(&mut self.rows, &row_key, &value_key);

        self.vs.remove(&value_key);

        if !self.columns.contains_key(&column_key) {
            self.cs.remove(&column_key);
        }

        if !self.rows.contains_key(&row_key) {
            self.rs.remove(&row_key);
        }
    }

    pub fn remove_by_row(&mut self, row_key_to_remove: usize) {
        let mut items_to_remove = HashSet::<(usize, usize)>::new();

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
    }

    pub fn remove_by_column(&mut self, column_key_to_remove: usize) {
        let mut items_to_remove = HashSet::<(usize, usize)>::new();

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
    pub column_value_keys_except: HashMap<(usize, usize), HashSet<usize>>,
    // This row has these values except those at intersection with this column
    pub row_value_keys_except: HashMap<(usize, usize), HashSet<usize>>,
}

impl InverseTable {
    pub fn rebuild_from<C: TableKV, R: TableKV, V: TableKV>(table: &Table<C, R, V>) -> Self {
        let mut column_value_keys_except = HashMap::<(usize, usize), HashSet<usize>>::new();
        let mut row_value_keys_except = HashMap::<(usize, usize), HashSet<usize>>::new();

        for key @ (column_key, row_key) in table.tuples.keys() {
            let column_value_keys = table.columns.get(column_key).unwrap();
            let row_value_keys = table.rows.get(row_key).unwrap();

            let column_values_diff = column_value_keys.difference(row_value_keys).cloned().collect();
            let row_values_diff = row_value_keys.difference(column_value_keys).cloned().collect();

            column_value_keys_except.insert(*key, column_values_diff);
            row_value_keys_except.insert(*key, row_values_diff);
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
    use super::*;

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct Container(usize);

    impl TableKV for Container {
        fn id(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn basic_ops() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container(1), Container(2), Container(11));
        table.insert(Container(1), Container(2), Container(11));
        table.insert(Container(1), Container(2), Container(12));
        table.insert(Container(1), Container(3), Container(12));

        assert_eq!(table.tuples.get(&(1, 2)).unwrap().len(), 2);
        assert_eq!(table.tuples.get(&(1, 3)).unwrap().len(), 1);
        assert!(table.tuples.get(&(4, 2)).is_none());
        table.remove_by_column(4);
        table.remove_by_column(1);
        assert!(table.is_empty());
    }

    #[test]
    fn remove_handle_duplicate() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container(1), Container(2), Container(11));
        table.insert(Container(1), Container(2), Container(11));
        table.remove(1, 2, 11);
        assert!(table.is_empty());
    }

    #[test]
    fn remove_by_column() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container(1), Container(2), Container(11));
        table.insert(Container(1), Container(2), Container(12));
        table.insert(Container(1), Container(3), Container(14));
        table.insert(Container(1), Container(3), Container(15));
        table.remove_by_column(1);
        assert!(table.is_empty());
    }

    #[test]
    fn remove_by_row() {
        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container(2), Container(1), Container(11));
        table.insert(Container(3), Container(1), Container(12));
        table.insert(Container(4), Container(1), Container(13));
        table.insert(Container(5), Container(1), Container(14));
        table.remove_by_row(1);
        assert!(table.is_empty());
    }

    #[test]
    fn inverse_table() {
        let mut hs1 = HashSet::<usize>::new();
        hs1.insert(12);
        hs1.insert(15);

        let mut hs2 = HashSet::<usize>::new();
        hs2.insert(17);

        let mut table: Table<Container, Container, Container> = Table::new();
        table.insert(Container(1), Container(2), Container(12));
        table.insert(Container(1), Container(3), Container(14));
        table.insert(Container(1), Container(2), Container(15));
        table.insert(Container(4), Container(5), Container(16));
        table.insert(Container(2), Container(5), Container(17));
        table.insert(Container(2), Container(6), Container(17));

        let inverse: InverseTable = InverseTable::rebuild_from(&table);
        assert_eq!(inverse.column_value_keys_except.get(&(1, 3)).unwrap(), &hs1);
        assert_eq!(inverse.row_value_keys_except.get(&(4, 5)).unwrap(), &hs2);
        assert!(inverse.row_value_keys_except.get(&(4, 6)).is_none());
    }
}