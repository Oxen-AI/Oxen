use liboxen::model::User;
use pyo3::prelude::*;

#[pyclass]
#[derive(Clone)]
pub struct PyUser {
    _user: User,
}

#[pymethods]
impl PyUser {
    #[new]
    #[pyo3(signature = (name, email))]
    pub fn new(name: String, email: String) -> Self {
        Self {
            _user: User { name, email },
        }
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self._user.name
    }

    #[getter]
    pub fn email(&self) -> &str {
        &self._user.email
    }

    fn __repr__(&self) -> String {
        format!(
            "PyUser(name='{}', email={})",
            self._user.name, self._user.email
        )
    }

    fn __str__(&self) -> String {
        self._user.name.to_string()
    }
}

impl From<User> for PyUser {
    fn from(user: User) -> PyUser {
        PyUser { _user: user }
    }
}
