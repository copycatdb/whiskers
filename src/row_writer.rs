//! PyRowWriter: Direct TDS wire → Python objects via tabby's RowWriter trait.
//!
//! Path: TDS bytes → decode_direct → CompactValue → PyObject
//! No SqlValue enum. No claw. Just tabby + whiskers.

use pyo3::prelude::*;
use pyo3::types::PyString;
use tabby::RowWriter;

/// Compact value — the only intermediate between TDS wire and Python.
#[derive(Clone)]
pub enum CompactValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    DateTime(i64),
    DateTimeOffset(i64, i16),
    Decimal(i128, u8, u8),
    Guid([u8; 16]),
}

/// Single result set: column info + flat CompactValue storage.
pub struct PyRowWriter {
    pub col_count: usize,
    pub values: Vec<CompactValue>,
}

impl PyRowWriter {
    pub fn new(col_count: usize) -> Self {
        Self {
            col_count,
            values: Vec::with_capacity(col_count * 256),
        }
    }

    pub fn row_count(&self) -> usize {
        if self.col_count == 0 {
            0
        } else {
            self.values.len() / self.col_count
        }
    }

    #[inline]
    pub fn get(&self, row: usize, col: usize) -> &CompactValue {
        &self.values[row * self.col_count + col]
    }
}

/// Lean row writer — no Option checks.
impl RowWriter for PyRowWriter {
    #[inline]
    fn write_null(&mut self, _col: usize) {
        self.values.push(CompactValue::Null);
    }
    #[inline]
    fn write_bool(&mut self, _col: usize, val: bool) {
        self.values.push(CompactValue::Bool(val));
    }
    #[inline]
    fn write_u8(&mut self, _col: usize, val: u8) {
        self.values.push(CompactValue::I64(val as i64));
    }
    #[inline]
    fn write_i16(&mut self, _col: usize, val: i16) {
        self.values.push(CompactValue::I64(val as i64));
    }
    #[inline]
    fn write_i32(&mut self, _col: usize, val: i32) {
        self.values.push(CompactValue::I64(val as i64));
    }
    #[inline]
    fn write_i64(&mut self, _col: usize, val: i64) {
        self.values.push(CompactValue::I64(val));
    }
    #[inline]
    fn write_f32(&mut self, _col: usize, val: f32) {
        self.values.push(CompactValue::F64(val as f64));
    }
    #[inline]
    fn write_f64(&mut self, _col: usize, val: f64) {
        self.values.push(CompactValue::F64(val));
    }
    #[inline]
    fn write_str(&mut self, _col: usize, val: &str) {
        self.values.push(CompactValue::Str(val.to_owned()));
    }
    #[inline]
    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        self.values.push(CompactValue::Bytes(val.to_owned()));
    }
    #[inline]
    fn write_date(&mut self, _col: usize, days: i32) {
        self.values.push(CompactValue::Date(days));
    }
    #[inline]
    fn write_time(&mut self, _col: usize, nanos: i64) {
        self.values.push(CompactValue::Time(nanos));
    }
    #[inline]
    fn write_datetime(&mut self, _col: usize, micros: i64) {
        self.values.push(CompactValue::DateTime(micros));
    }
    #[inline]
    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, offset_minutes: i16) {
        self.values
            .push(CompactValue::DateTimeOffset(micros, offset_minutes));
    }
    #[inline]
    fn write_decimal(&mut self, _col: usize, value: i128, precision: u8, scale: u8) {
        self.values
            .push(CompactValue::Decimal(value, precision, scale));
    }
    #[inline]
    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        self.values.push(CompactValue::Guid(*bytes));
    }
    #[inline]
    fn write_utf16(&mut self, _col: usize, val: &[u16]) {
        self.values
            .push(CompactValue::Str(String::from_utf16_lossy(val)));
    }
}

/// Direct-to-PyObject row writer. Writes PyObjects during TDS decode,
/// eliminating the CompactValue intermediate for the hot path.
/// MUST be used with the GIL held.
pub struct DirectPyWriter {
    pub col_count: usize,
    pub py_values: Vec<PyObject>,
    pub messages: Vec<(String, String)>,
    py: Python<'static>, // Lifetime elided — only valid while GIL is held
}

impl DirectPyWriter {
    /// # Safety
    /// The `py` token must remain valid for the entire lifetime of this writer.
    pub unsafe fn new(py: Python<'_>, col_count: usize) -> Self {
        Self {
            col_count,
            py_values: Vec::with_capacity(col_count * 256),
            messages: Vec::new(),
            py: unsafe { std::mem::transmute::<Python<'_>, Python<'static>>(py) },
        }
    }

    pub fn row_count(&self) -> usize {
        if self.col_count == 0 {
            0
        } else {
            self.py_values.len() / self.col_count
        }
    }
}

impl RowWriter for DirectPyWriter {
    #[inline]
    fn write_null(&mut self, _col: usize) {
        unsafe {
            let ptr = pyo3::ffi::Py_None();
            pyo3::ffi::Py_IncRef(ptr);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_bool(&mut self, _col: usize, val: bool) {
        unsafe {
            let ptr = if val {
                pyo3::ffi::Py_True()
            } else {
                pyo3::ffi::Py_False()
            };
            pyo3::ffi::Py_IncRef(ptr);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_u8(&mut self, _col: usize, val: u8) {
        // Raw CPython API — avoid PyO3 into_pyobject chain overhead
        unsafe {
            let ptr = pyo3::ffi::PyLong_FromLongLong(val as std::ffi::c_longlong);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_i16(&mut self, _col: usize, val: i16) {
        unsafe {
            let ptr = pyo3::ffi::PyLong_FromLongLong(val as std::ffi::c_longlong);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_i32(&mut self, _col: usize, val: i32) {
        unsafe {
            let ptr = pyo3::ffi::PyLong_FromLongLong(val as std::ffi::c_longlong);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_i64(&mut self, _col: usize, val: i64) {
        unsafe {
            let ptr = pyo3::ffi::PyLong_FromLongLong(val as std::ffi::c_longlong);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_f32(&mut self, _col: usize, val: f32) {
        unsafe {
            let ptr = pyo3::ffi::PyFloat_FromDouble(val as f64);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_f64(&mut self, _col: usize, val: f64) {
        unsafe {
            let ptr = pyo3::ffi::PyFloat_FromDouble(val);
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_str(&mut self, _col: usize, val: &str) {
        unsafe {
            let ptr = pyo3::ffi::PyUnicode_FromStringAndSize(
                val.as_ptr() as *const std::ffi::c_char,
                val.len() as pyo3::ffi::Py_ssize_t,
            );
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        unsafe {
            let ptr = pyo3::ffi::PyBytes_FromStringAndSize(
                val.as_ptr() as *const std::ffi::c_char,
                val.len() as pyo3::ffi::Py_ssize_t,
            );
            self.py_values.push(PyObject::from_owned_ptr(self.py, ptr));
        }
    }
    #[inline]
    fn write_date(&mut self, _col: usize, days: i32) {
        // Store as CompactValue-equivalent for later conversion
        // (datetime module import needed — too expensive per-cell)
        self.py_values
            .push(crate::types::date_days_to_py(self.py, days).unwrap_or_else(|_| self.py.None()));
    }
    #[inline]
    fn write_time(&mut self, _col: usize, nanos: i64) {
        self.py_values.push(
            crate::types::time_nanos_to_py(self.py, nanos).unwrap_or_else(|_| self.py.None()),
        );
    }
    #[inline]
    fn write_datetime(&mut self, _col: usize, micros: i64) {
        self.py_values.push(
            crate::types::datetime_micros_to_py(self.py, micros).unwrap_or_else(|_| self.py.None()),
        );
    }
    #[inline]
    fn write_datetimeoffset(&mut self, _col: usize, micros: i64, offset_minutes: i16) {
        self.py_values.push(
            crate::types::datetimeoffset_to_py(self.py, micros, offset_minutes)
                .unwrap_or_else(|_| self.py.None()),
        );
    }
    #[inline]
    fn write_decimal(&mut self, _col: usize, value: i128, precision: u8, scale: u8) {
        self.py_values.push(
            crate::types::decimal_to_py(self.py, value, precision, scale)
                .unwrap_or_else(|_| self.py.None()),
        );
    }
    #[inline]
    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        self.py_values
            .push(crate::types::guid_to_py(self.py, bytes).unwrap_or_else(|_| self.py.None()));
    }
    #[inline]
    fn write_utf16(&mut self, _col: usize, val: &[u16]) {
        let s = String::from_utf16_lossy(val);
        self.py_values
            .push(PyString::new(self.py, &s).into_any().unbind());
    }
    fn on_info(&mut self, number: u32, message: &str) {
        let header = format!("[01000] ({})", number);
        self.messages.push((header, message.to_owned()));
    }
}

/// Multi-result-set writer: accumulates complete result sets via on_metadata/on_row_done.
/// Used with tabby's batch_into() which keeps a single writer across all result sets.
pub struct MultiSetWriter {
    pub completed: Vec<(Vec<crate::cursor::ColumnInfo>, PyRowWriter)>,
    pub messages: Vec<(String, String)>,
    current_cols: Option<Vec<crate::cursor::ColumnInfo>>,
    current: Option<PyRowWriter>,
}

impl Default for MultiSetWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiSetWriter {
    pub fn new() -> Self {
        Self {
            completed: Vec::new(),
            messages: Vec::new(),
            current_cols: None,
            current: None,
        }
    }

    pub fn finalize(mut self) -> Vec<(Vec<crate::cursor::ColumnInfo>, PyRowWriter)> {
        if let (Some(cols), Some(writer)) = (self.current_cols.take(), self.current.take()) {
            self.completed.push((cols, writer));
        }
        self.completed
    }
}

impl RowWriter for MultiSetWriter {
    fn on_metadata(&mut self, columns: &[tabby::Column]) {
        if let (Some(cols), Some(writer)) = (self.current_cols.take(), self.current.take()) {
            self.completed.push((cols, writer));
        }
        let col_infos: Vec<crate::cursor::ColumnInfo> = columns
            .iter()
            .map(crate::cursor::TdsCursor::column_to_info)
            .collect();
        let col_count = columns.len();
        self.current_cols = Some(col_infos);
        self.current = Some(PyRowWriter::new(col_count));
    }

    fn on_row_done(&mut self) {}

    fn on_info(&mut self, number: u32, message: &str) {
        let header = format!("[01000] ({})", number);
        self.messages.push((header, message.to_owned()));
    }

    #[inline]
    fn write_null(&mut self, col: usize) {
        if let Some(ref mut w) = self.current {
            w.write_null(col);
        }
    }
    #[inline]
    fn write_bool(&mut self, col: usize, val: bool) {
        if let Some(ref mut w) = self.current {
            w.write_bool(col, val);
        }
    }
    #[inline]
    fn write_u8(&mut self, col: usize, val: u8) {
        if let Some(ref mut w) = self.current {
            w.write_u8(col, val);
        }
    }
    #[inline]
    fn write_i16(&mut self, col: usize, val: i16) {
        if let Some(ref mut w) = self.current {
            w.write_i16(col, val);
        }
    }
    #[inline]
    fn write_i32(&mut self, col: usize, val: i32) {
        if let Some(ref mut w) = self.current {
            w.write_i32(col, val);
        }
    }
    #[inline]
    fn write_i64(&mut self, col: usize, val: i64) {
        if let Some(ref mut w) = self.current {
            w.write_i64(col, val);
        }
    }
    #[inline]
    fn write_f32(&mut self, col: usize, val: f32) {
        if let Some(ref mut w) = self.current {
            w.write_f32(col, val);
        }
    }
    #[inline]
    fn write_f64(&mut self, col: usize, val: f64) {
        if let Some(ref mut w) = self.current {
            w.write_f64(col, val);
        }
    }
    #[inline]
    fn write_str(&mut self, col: usize, val: &str) {
        if let Some(ref mut w) = self.current {
            w.write_str(col, val);
        }
    }
    #[inline]
    fn write_bytes(&mut self, col: usize, val: &[u8]) {
        if let Some(ref mut w) = self.current {
            w.write_bytes(col, val);
        }
    }
    #[inline]
    fn write_date(&mut self, col: usize, days: i32) {
        if let Some(ref mut w) = self.current {
            w.write_date(col, days);
        }
    }
    #[inline]
    fn write_time(&mut self, col: usize, nanos: i64) {
        if let Some(ref mut w) = self.current {
            w.write_time(col, nanos);
        }
    }
    #[inline]
    fn write_datetime(&mut self, col: usize, micros: i64) {
        if let Some(ref mut w) = self.current {
            w.write_datetime(col, micros);
        }
    }
    #[inline]
    fn write_datetimeoffset(&mut self, col: usize, micros: i64, offset_minutes: i16) {
        if let Some(ref mut w) = self.current {
            w.write_datetimeoffset(col, micros, offset_minutes);
        }
    }
    #[inline]
    fn write_decimal(&mut self, col: usize, value: i128, precision: u8, scale: u8) {
        if let Some(ref mut w) = self.current {
            w.write_decimal(col, value, precision, scale);
        }
    }
    #[inline]
    fn write_guid(&mut self, col: usize, bytes: &[u8; 16]) {
        if let Some(ref mut w) = self.current {
            w.write_guid(col, bytes);
        }
    }
    #[inline]
    fn write_utf16(&mut self, col: usize, val: &[u16]) {
        if let Some(ref mut w) = self.current {
            w.write_utf16(col, val);
        }
    }
}
