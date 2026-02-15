use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::connection::SharedClient;
use crate::errors::to_pyerr;
use crate::row_writer::{CompactValue, PyRowWriter};
use crate::types::{column_type_to_sql_type, compact_value_to_py, py_to_sql_literal};
use std::sync::{Arc, Mutex};

fn convert_call_syntax(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        let inner = trimmed[1..trimmed.len() - 1].trim();
        if inner.to_uppercase().starts_with("CALL ") {
            let rest = inner[5..].trim();
            if let Some(paren_idx) = rest.find('(') {
                let proc_name = &rest[..paren_idx];
                let args = &rest[paren_idx + 1..rest.len() - 1];
                return format!("EXEC {} {}", proc_name.trim(), args.trim());
            } else {
                return format!("EXEC {}", rest);
            }
        }
    }
    sql.to_string()
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: i32,
    pub column_size: i64,
    pub decimal_digits: i32,
    pub nullable: i32,
}

pub struct TransactionState {
    pub autocommit: bool,
    pub in_transaction: bool,
}

pub type SharedTxState = Arc<Mutex<TransactionState>>;

struct ResultSet {
    columns: Vec<ColumnInfo>,
    writer: PyRowWriter,
}

pub struct TdsCursor {
    client: SharedClient,
    tx_state: SharedTxState,
    columns: Option<Vec<ColumnInfo>>,
    writer: Option<PyRowWriter>,
    /// Direct row tuples — pre-built during TDS decode
    direct_rows: Option<Vec<PyObject>>,
    direct_col_count: usize,
    row_index: usize,
    _rowcount: i64,
    pending: Vec<ResultSet>,
    messages: Vec<(String, String)>,
}

impl TdsCursor {
    pub fn new(client: SharedClient, tx_state: SharedTxState) -> Self {
        TdsCursor {
            client,
            tx_state,
            columns: None,
            writer: None,
            direct_rows: None,
            direct_col_count: 0,
            row_index: 0,
            _rowcount: -1,
            pending: Vec::new(),
            messages: Vec::new(),
        }
    }

    pub fn close(&mut self) -> PyResult<()> {
        self.columns = None;
        self.writer = None;
        self.direct_rows = None;
        self.pending.clear();
        Ok(())
    }

    fn begin_transaction_if_needed(&self) -> PyResult<Option<String>> {
        let mut state = self.tx_state.lock().unwrap();
        if !state.autocommit && !state.in_transaction {
            state.in_transaction = true;
            Ok(Some("BEGIN TRANSACTION\n".to_string()))
        } else {
            Ok(None)
        }
    }

    pub fn execute(&mut self, sql: &str, params: &[Bound<'_, PyAny>]) -> PyResult<i32> {
        let sql = convert_call_syntax(sql);
        let final_sql = if params.is_empty() {
            sql.to_string()
        } else {
            Python::with_gil(|py| substitute_params(py, &sql, params))?
        };

        let tx_prefix = self.begin_transaction_if_needed()?;
        let client = self.client.clone();

        self.columns = None;
        self.writer = None;
        self.direct_rows = None;
        self.direct_col_count = 0;
        self.row_index = 0;
        self._rowcount = -1;
        self.pending.clear();

        self.execute_direct(client, &final_sql, tx_prefix)
    }

    /// Two-phase execute:
    /// Phase 1: TDS decode → CompactValues (GIL released for max throughput)
    /// Phase 2: CompactValues → PyObject tuples (GIL held, raw CPython API)
    fn execute_direct(
        &mut self,
        client: SharedClient,
        final_sql: &str,
        tx_prefix: Option<String>,
    ) -> PyResult<i32> {
        let trimmed_upper = final_sql.trim().to_uppercase();

        let needs_rowcount = trimmed_upper.starts_with("INSERT ")
            || trimmed_upper.starts_with("UPDATE ")
            || trimmed_upper.starts_with("DELETE ")
            || trimmed_upper.starts_with("TRUNCATE ")
            || trimmed_upper.starts_with("MERGE ");

        let skip_rowcount = trimmed_upper.starts_with("CREATE VIEW")
            || trimmed_upper.starts_with("ALTER VIEW")
            || trimmed_upper.starts_with("CREATE TRIGGER")
            || trimmed_upper.starts_with("ALTER TRIGGER")
            || trimmed_upper.starts_with("CREATE FUNCTION")
            || trimmed_upper.starts_with("ALTER FUNCTION")
            || trimmed_upper.starts_with("CREATE PROCEDURE")
            || trimmed_upper.starts_with("ALTER PROCEDURE")
            || trimmed_upper.starts_with("CREATE PROC ")
            || trimmed_upper.starts_with("ALTER PROC ")
            || trimmed_upper.starts_with("CREATE OR ALTER");

        let mut batch_sql =
            String::with_capacity(final_sql.len() + tx_prefix.as_ref().map_or(0, |p| p.len()) + 40);
        if let Some(prefix) = tx_prefix {
            batch_sql.push_str(&prefix);
        }
        batch_sql.push_str(final_sql);
        if needs_rowcount && !skip_rowcount {
            batch_sql.push_str("\nSELECT @@ROWCOUNT AS __rowcount__");
        }

        // Phase 1: TDS decode → CompactValues (GIL released)
        let decode_result = Python::with_gil(|py| {
            py.allow_threads(|| {
                let mut c = client.lock().unwrap();
                let mut string_buf = String::with_capacity(4096);
                let mut bytes_buf = Vec::with_capacity(4096);

                let columns = c.batch_start(&batch_sql).map_err(to_pyerr)?;

                if columns.is_empty() {
                    let _ = c.batch_drain();
                    return Ok::<_, PyErr>(None);
                }

                let col_infos: Vec<ColumnInfo> =
                    columns.iter().map(TdsCursor::column_to_info).collect();
                let col_count = columns.len();

                // First result set
                let mut writer = PyRowWriter::new(col_count);
                let mut has_more = false;
                loop {
                    match c
                        .batch_fetch_row(&mut writer, &mut string_buf, &mut bytes_buf)
                        .map_err(to_pyerr)?
                    {
                        tabby::BatchFetchResult::Row => {}
                        tabby::BatchFetchResult::MoreResults => {
                            has_more = true;
                            break;
                        }
                        tabby::BatchFetchResult::Done(_) => break,
                    }
                }

                // Additional result sets
                let mut extra_sets = Vec::new();
                if has_more {
                    loop {
                        let next_cols = c.batch_fetch_metadata().map_err(to_pyerr)?;
                        if next_cols.is_empty() {
                            break;
                        }
                        let next_infos: Vec<ColumnInfo> =
                            next_cols.iter().map(TdsCursor::column_to_info).collect();
                        let mut rw = PyRowWriter::new(next_cols.len());
                        loop {
                            match c
                                .batch_fetch_row(&mut rw, &mut string_buf, &mut bytes_buf)
                                .map_err(to_pyerr)?
                            {
                                tabby::BatchFetchResult::Row => {}
                                tabby::BatchFetchResult::MoreResults
                                | tabby::BatchFetchResult::Done(_) => break,
                            }
                        }
                        extra_sets.push((next_infos, rw));
                    }
                }

                Ok(Some((col_infos, col_count, writer, extra_sets)))
            })
        })?;

        let Some((col_infos, col_count, writer, extra_sets)) = decode_result else {
            self.columns = None;
            self.writer = None;
            self.direct_rows = None;
            self._rowcount = 0;
            self.row_index = 0;
            self.pending.clear();
            return Ok(0);
        };

        // Phase 2: CompactValues → PyObject tuples (GIL held, raw CPython API)
        let check_rowcount = needs_rowcount && !skip_rowcount;
        let row_count = writer.row_count();

        // Check for __rowcount__
        if check_rowcount && col_infos.len() == 1 && col_infos[0].name == "__rowcount__" {
            if row_count > 0 {
                if let CompactValue::I64(v) = writer.get(0, 0) {
                    self._rowcount = *v;
                }
            } else {
                self._rowcount = 0;
            }
            self.columns = None;
            self.writer = None;
            self.direct_rows = None;
            self.row_index = 0;

            // Process extra sets
            let mut pending: Vec<ResultSet> = Vec::new();
            for (infos, rw) in extra_sets {
                if check_rowcount && infos.len() == 1 && infos[0].name == "__rowcount__" {
                    continue;
                }
                pending.push(ResultSet {
                    columns: infos,
                    writer: rw,
                });
            }
            if !pending.is_empty() {
                let first = pending.remove(0);
                self.columns = Some(first.columns);
                self.writer = Some(first.writer);
                self._rowcount = -1;
            }
            self.pending = pending;
            return Ok(0);
        }

        // Store CompactValues — convert to PyObjects lazily in fetchall/fetchone
        self.columns = Some(col_infos);
        self.writer = Some(writer);
        self.direct_rows = None;
        self.direct_col_count = col_count;
        self._rowcount = -1;
        self.row_index = 0;

        self.pending = extra_sets
            .into_iter()
            .map(|(infos, rw)| ResultSet {
                columns: infos,
                writer: rw,
            })
            .collect();
        self.messages.clear();

        Ok(0)
    }

    #[allow(dead_code)]
    fn process_results(
        &mut self,
        results: Vec<(Vec<ColumnInfo>, PyRowWriter)>,
        check_rowcount: bool,
    ) -> PyResult<i32> {
        let mut all_sets: Vec<ResultSet> = Vec::new();
        let mut rowcount_from_batch: Option<i64> = None;

        for (cols, writer) in results {
            if cols.is_empty() {
                continue;
            }

            if check_rowcount && cols.len() == 1 && cols[0].name == "__rowcount__" {
                if writer.row_count() > 0 {
                    if let CompactValue::I64(v) = writer.get(0, 0) {
                        rowcount_from_batch = Some(*v);
                    }
                }
                continue;
            }

            all_sets.push(ResultSet {
                columns: cols,
                writer,
            });
        }

        if !all_sets.is_empty() {
            let first = all_sets.remove(0);
            self.columns = Some(first.columns);
            self.writer = Some(first.writer);
            self._rowcount = -1;
            self.pending = all_sets;
        } else {
            self.columns = None;
            self.writer = None;
            self._rowcount = rowcount_from_batch.unwrap_or(0);
        }

        Ok(0)
    }

    #[inline]
    fn row_to_py(&self, py: Python<'_>, row_idx: usize) -> PyResult<Vec<PyObject>> {
        let writer = self.writer.as_ref().unwrap();
        let col_count = writer.col_count;
        let mut py_row = Vec::with_capacity(col_count);
        for c in 0..col_count {
            py_row.push(compact_value_to_py(py, writer.get(row_idx, c))?);
        }
        Ok(py_row)
    }

    pub fn column_to_info(c: &tabby::Column) -> ColumnInfo {
        let type_name = format!("{:?}", c.column_type());
        let sql_type = column_type_to_sql_type(&type_name);

        let (column_size, decimal_digits, nullable) = if let Some(ti) = c.type_info() {
            use tabby::DataType;
            match ti {
                DataType::FixedLen(ft) => {
                    use tabby::FixedLenType;
                    let size = match ft {
                        FixedLenType::Null => 0,
                        FixedLenType::Bit => 1,
                        FixedLenType::Int1 => 3,
                        FixedLenType::Int2 => 5,
                        FixedLenType::Int4 => 10,
                        FixedLenType::Int8 => 19,
                        FixedLenType::Float4 => 24,
                        FixedLenType::Float8 => 53,
                        FixedLenType::Datetime4 => 16,
                        FixedLenType::Datetime => 23,
                        FixedLenType::Money4 => 10,
                        FixedLenType::Money => 19,
                    };
                    (size as i64, 0i32, c.nullable().unwrap_or(true))
                }
                DataType::VarLenSized(ctx) => {
                    use tabby::VarLenType;
                    let len = ctx.len() as i64;
                    let size = match ctx.r#type() {
                        VarLenType::Intn => match ctx.len() {
                            1 => 3,
                            2 => 5,
                            4 => 10,
                            _ => 19,
                        },
                        VarLenType::Bitn => 1,
                        VarLenType::Floatn => match ctx.len() {
                            4 => 24,
                            _ => 53,
                        },
                        VarLenType::Guid => 36,
                        VarLenType::NVarchar | VarLenType::NChar => {
                            if len > 4000 {
                                0
                            } else {
                                len / 2
                            }
                        }
                        VarLenType::BigVarChar | VarLenType::BigChar => {
                            if len > 8000 {
                                0
                            } else {
                                len
                            }
                        }
                        VarLenType::BigVarBin | VarLenType::BigBinary => {
                            if len > 8000 {
                                0
                            } else {
                                len
                            }
                        }
                        VarLenType::Datetimen => match ctx.len() {
                            4 => 16,
                            _ => 23,
                        },
                        VarLenType::Daten => 10,
                        VarLenType::Timen => 8 + ctx.len() as i64,
                        VarLenType::Datetime2 => {
                            19 + if ctx.len() > 0 {
                                1 + ctx.len() as i64
                            } else {
                                0
                            }
                        }
                        VarLenType::DatetimeOffsetn => {
                            26 + if ctx.len() > 0 {
                                1 + ctx.len() as i64
                            } else {
                                0
                            }
                        }
                        VarLenType::Money => 19,
                        VarLenType::Text | VarLenType::NText => 0,
                        VarLenType::Image => 0,
                        VarLenType::Xml => 0,
                        _ => len,
                    };
                    (size, 0, c.nullable().unwrap_or(true))
                }
                DataType::VarLenSizedPrecision {
                    precision, scale, ..
                } => (
                    *precision as i64,
                    *scale as i32,
                    c.nullable().unwrap_or(true),
                ),
                DataType::Xml { .. } => (0, 0, c.nullable().unwrap_or(true)),
            }
        } else {
            (0, 0, true)
        };

        ColumnInfo {
            name: c.name().to_string(),
            sql_type,
            column_size,
            decimal_digits,
            nullable: if nullable { 1 } else { 0 },
        }
    }

    pub fn execute_many(
        &mut self,
        sql: &str,
        columnwise_params: &[Bound<'_, PyList>],
        row_count: usize,
    ) -> PyResult<i32> {
        let mut total_affected: i64 = 0;
        for row_idx in 0..row_count {
            let params: Vec<PyObject> = Python::with_gil(|_py| -> PyResult<Vec<PyObject>> {
                let mut row_params = Vec::new();
                for col in columnwise_params {
                    let val = col.get_item(row_idx)?;
                    row_params.push(val.unbind());
                }
                Ok(row_params)
            })?;
            Python::with_gil(|py| -> PyResult<()> {
                let bound_params: Vec<Bound<'_, PyAny>> =
                    params.iter().map(|p| p.bind(py).clone()).collect();
                self.execute(sql, &bound_params)?;
                if self._rowcount >= 0 {
                    total_affected += self._rowcount;
                }
                Ok(())
            })?;
        }
        self._rowcount = total_affected;
        Ok(0)
    }

    pub fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Vec<PyObject>>> {
        // Fast path: pre-built tuples
        if let Some(ref rows) = self.direct_rows {
            if self.row_index < rows.len() {
                // Return the tuple's elements as a Vec for compatibility
                let tuple = rows[self.row_index].bind(py);
                let col_count = self.direct_col_count;
                let mut row = Vec::with_capacity(col_count);
                for c in 0..col_count {
                    unsafe {
                        let item =
                            pyo3::ffi::PyTuple_GET_ITEM(tuple.as_ptr(), c as pyo3::ffi::Py_ssize_t);
                        pyo3::ffi::Py_INCREF(item);
                        row.push(PyObject::from_owned_ptr(py, item));
                    }
                }
                self.row_index += 1;
                return Ok(Some(row));
            }
            return Ok(None);
        }

        if let Some(ref writer) = self.writer {
            if self.row_index < writer.row_count() {
                let row = self.row_to_py(py, self.row_index)?;
                self.row_index += 1;
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    pub fn fetchmany(&mut self, py: Python<'_>, size: usize) -> PyResult<Vec<Vec<PyObject>>> {
        let total = self.writer.as_ref().map_or(0, |w| w.row_count());
        let end = std::cmp::min(self.row_index + size, total);
        let mut result = Vec::with_capacity(end - self.row_index);
        for i in self.row_index..end {
            result.push(self.row_to_py(py, i)?);
        }
        self.row_index = end;
        Ok(result)
    }

    pub fn fetchall(&mut self, py: Python<'_>) -> PyResult<Vec<Vec<PyObject>>> {
        let writer = match self.writer.as_ref() {
            Some(w) => w,
            None => return Ok(Vec::new()),
        };
        let total = writer.row_count();
        let remaining = total - self.row_index;
        let col_count = writer.col_count;
        let values = &writer.values;
        let mut result = Vec::with_capacity(remaining);
        for i in self.row_index..total {
            let base = i * col_count;
            let mut py_row = Vec::with_capacity(col_count);
            for c in 0..col_count {
                py_row.push(compact_value_to_py(py, &values[base + c])?);
            }
            result.push(py_row);
        }
        self.row_index = total;
        Ok(result)
    }

    /// Optimized fetchall that writes directly into a PyList of PyLists,
    /// avoiding intermediate Vec<Vec<PyObject>> allocation.
    pub fn fetchall_into(
        &mut self,
        py: Python<'_>,
        rows_data: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        // Fast path: pre-built row tuples (from DirectPyWriter)
        if let Some(ref rows) = self.direct_rows {
            let total = rows.len();
            for i in self.row_index..total {
                unsafe {
                    pyo3::ffi::PyList_Append(rows_data.as_ptr(), rows[i].as_ptr());
                }
            }
            self.row_index = total;
            return Ok(());
        }

        // Fallback: CompactValue storage (multi-result-set pending sets)
        let writer = match self.writer.as_ref() {
            Some(w) => w,
            None => return Ok(()),
        };
        let total = writer.row_count();
        let col_count = writer.col_count;
        let values = &writer.values;
        for i in self.row_index..total {
            let base = i * col_count;
            let row_list = pyo3::types::PyList::new(
                py,
                (0..col_count)
                    .map(|c| compact_value_to_py(py, &values[base + c]))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
            rows_data.append(row_list)?;
        }
        self.row_index = total;
        Ok(())
    }

    pub fn nextset(&mut self) -> PyResult<bool> {
        if self.pending.is_empty() {
            self.columns = None;
            self.writer = None;
            self.direct_rows = None;
            self.row_index = 0;
            return Ok(false);
        }
        let next = self.pending.remove(0);
        self.columns = Some(next.columns);
        self.writer = Some(next.writer);
        self.direct_rows = None;
        self.row_index = 0;
        self._rowcount = -1;
        Ok(true)
    }

    pub fn rowcount(&self) -> i64 {
        self._rowcount
    }
    pub fn description(&self) -> Option<&Vec<ColumnInfo>> {
        self.columns.as_ref()
    }
    pub fn row_count_total(&self) -> usize {
        if let Some(ref rows) = self.direct_rows {
            return rows.len();
        }
        self.writer.as_ref().map_or(0, |w| w.row_count())
    }
    pub fn current_row_index(&self) -> usize {
        self.row_index
    }
    pub fn set_row_index(&mut self, idx: usize) {
        self.row_index = idx;
    }

    pub fn direct_rows(&self) -> &Option<Vec<PyObject>> {
        &self.direct_rows
    }

    /// Build a PyTuple for a single row from CompactValue writer (fallback path)
    pub fn row_to_py_tuple(&self, py: Python<'_>, row_idx: usize) -> PyResult<PyObject> {
        let writer = self.writer.as_ref().unwrap();
        let col_count = writer.col_count;
        let values = &writer.values;
        let base = row_idx * col_count;
        unsafe {
            let tuple = pyo3::ffi::PyTuple_New(col_count as pyo3::ffi::Py_ssize_t);
            for c in 0..col_count {
                let obj = compact_value_to_py(py, &values[base + c])?;
                pyo3::ffi::PyTuple_SET_ITEM(tuple, c as pyo3::ffi::Py_ssize_t, obj.into_ptr());
            }
            Ok(PyObject::from_owned_ptr(py, tuple))
        }
    }
    pub fn get_messages(&self) -> &[(String, String)] {
        &self.messages
    }
}

fn substitute_params(py: Python<'_>, sql: &str, params: &[Bound<'_, PyAny>]) -> PyResult<String> {
    let mut result = String::with_capacity(sql.len() + params.len() * 16);
    let mut param_idx = 0;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '?' {
            if param_idx < params.len() {
                let literal = py_to_sql_literal(py, &params[param_idx])?;
                result.push_str(&literal);
                param_idx += 1;
            } else {
                result.push('?');
            }
        } else if c == '\'' {
            result.push(c);
            while let Some(sc) = chars.next() {
                result.push(sc);
                if sc == '\'' {
                    if chars.peek() == Some(&'\'') {
                        result.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }
    Ok(result)
}
