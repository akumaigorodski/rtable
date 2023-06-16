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

#[derive(Clone, Debug)]
pub struct InverseTable<'a, C: Eq + Hash, R: Eq + Hash, V: Eq + Hash> {
    columns_except: HashMap<(&'a C, &'a R), HashSet<&'a V>>,
    rows_except: HashMap<(&'a C, &'a R), HashSet<&'a V>>,
}

impl<'a, C: Eq + Hash, R: Eq + Hash, V: Eq + Hash> Table<'a, C, R, V> {
    pub fn new() -> Self {
        Table {
            tuples: HashMap::new(),
            columns: HashMap::new(),
            rows: HashMap::new(),
        }
    }

    pub fn strict_insert(&mut self, column: &'a C, row: &'a R, value: &'a V) -> &mut Self {
        assert_eq!(self.tuples.get(&(column, row)).map(|set| set.contains(value)).unwrap_or(false), false);
        self.insert(column, row, value)
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

        for (tuple, values) in &self.tuples {
            if values.contains(value_to_remove) {
                items_to_remove.insert(*tuple);
            }
        }

        for (column, row_to_remove) in items_to_remove {
            self.remove(column, row_to_remove, value_to_remove);
        }

        self
    }

    pub fn remove_by_row(&mut self, row_to_remove: &'a R) -> &mut Self {
        let mut items_to_remove = HashSet::<(&C, &V)>::new();

        for (tuple, vals) in &self.tuples {
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

        for (tuple, vals) in &self.tuples {
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

impl<'a, C: Eq + Hash, R: Eq + Hash, V: Eq + Hash> InverseTable<'a, C, R, V> {
    // This constructs sets of values that each row and column have except where they intersect

    pub fn rebuild_from(table: &Table<'a, C, R, V>) -> Self {
        let mut columns_except = HashMap::<(&'a C, &'a R), HashSet<&'a V>>::new();
        let mut rows_except = HashMap::<(&'a C, &'a R), HashSet<&'a V>>:: new();

        for &key @ (column, row) in table.tuples.keys() {
            let column_values = table.columns.get(column).unwrap();
            let row_values = table.rows.get(row).unwrap();

            let column_values_diff = column_values.difference(row_values).map(|&value| value).collect();
            let row_values_diff = row_values.difference(column_values).map(|&value| value).collect();

            columns_except.insert(key, column_values_diff);
            rows_except.insert(key, row_values_diff);
        }

        InverseTable {
            columns_except,
            rows_except
        }
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

    #[test]
    fn inverse_table() {
        let mut hs1 = HashSet::<&i32>::new();
        hs1.insert(&12);
        hs1.insert(&15);

        let mut hs2 = HashSet::<&i32>::new();
        hs2.insert(&16);

        let mut table: Table<&str, &str, i32> = Table::new();
        table.insert(&"a", &"b", &12);
        table.insert(&"a", &"c", &14);
        table.insert(&"a", &"d", &15);
        table.insert(&"e", &"b", &16);
        let inverse: InverseTable<&str, &str, i32> = InverseTable::rebuild_from(&table);
        assert_eq!(inverse.columns_except.get(&(&"a", &"c")).unwrap(), &hs1);
        assert_eq!(inverse.rows_except.get(&(&"a", &"b")).unwrap(), &hs2);
    }
}