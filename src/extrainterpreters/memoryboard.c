#include <Python.h>
#include <stdatomic.h>

PyDoc_STRVAR(_memoryboard_remote_memory_doc,
"remote_memory(buffer_address, buffer_length)\n\
\n\
For internal extrainterpreters use only!\n\n\
\
Returns a writable memoryview object pointing to \
the indicated memory. \n\
\n\
THIS IS UNSAFE AND _WILL_ CRASH THE PROCESS IF USED INCORRECTLY.\
");


static PyObject *_memoryboard_remote_memory(PyObject *self, PyObject *args)
{
    unsigned long size;
    Py_ssize_t tmp;
    char *memory;

    if (!PyArg_ParseTuple(args, "nk", &tmp, &size)) {
        return NULL;
    }

    if (tmp == NULL) {
        return NULL;
    }

    memory = (char *)tmp;

    return PyMemoryView_FromMemory(memory, size, PyBUF_WRITE);

}


PyDoc_STRVAR(_memoryboard_get_address_and_size_doc,
"getaddress_and_size(buffer_obj) -> (buffer_address, buffer_lenght)\n\
\n\
Returns the memory address and lenght of an object that \
implements the buffer protocol. The return is suitable \
as input of remote_memory.\n\
However, unlike high-level Python objects, the source\n\
object must not be disposed or reallocated if the \n\
memoryview returned by remote_memory is in use.\
");

static PyObject *_memoryboard_get_address_and_size(PyObject *self, PyObject *args)
{
    Py_buffer buffer;

    if (!PyArg_ParseTuple(args, "y*", &buffer)) {
        return NULL;
    }

    return Py_BuildValue(
      "(OO)",
     PyLong_FromSsize_t((Py_ssize_t)buffer.buf),
     PyLong_FromLong(buffer.len)
    );
}

PyDoc_STRVAR(_atomic_byte_loc_doc,
"_atomic_byte_loc(byte_address) -> bool\n\
\n\
Returns true if the byte at given address is incread from 0 to 1\n\n\
\
(addressable with the _memoryboard_get_address_and_size protocols \
or ctypes) is successfully atomically found to be zero, and increased to one \
using standard C functionality for atomic data.\n\n\
\
This call can be used to build absolute locks accross\n\
interpreters and threads in pure Python.\n\
");

static PyObject *_atomic_byte_loc(PyObject *self, PyObject *args)
{
    Py_ssize_t address;
    atomic_char *target;

    if (!PyArg_ParseTuple(args, "n", &address)) {
        return NULL;
    }

    target = (atomic_char *) address;
    if (*target == 0 && (++*target) == 1) {
        Py_RETURN_TRUE;
    }

    Py_RETURN_FALSE;
}

static PyObject *_object_from_id(PyObject *self, PyObject *args)
{
    Py_ssize_t address;
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "n", &address)) {
        return NULL;
    }

    obj = (PyObject *) address;
    Py_INCREF(obj);

    return obj;
}

static PyMethodDef _memoryboard_methods[] = {
    {"_remote_memory", _memoryboard_remote_memory, METH_VARARGS, _memoryboard_remote_memory_doc},
    {"_address_and_size", _memoryboard_get_address_and_size, METH_VARARGS, _memoryboard_get_address_and_size_doc},
    {"_atomic_byte_lock", _atomic_byte_loc, METH_VARARGS, _atomic_byte_loc_doc},
    {"_object_from_id", _object_from_id, METH_VARARGS, "Swift death. Do not use."},
    {NULL, NULL, 0, NULL}
};

PyDoc_STRVAR(module_doc,
"Native functions for extrainterpreters usage.");

static int
_memoryboard_modexec(PyObject *m)
{
    return 0;
}


static PyModuleDef_Slot _memoryboard_slots[] = {
    {Py_mod_exec, _memoryboard_modexec},
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#if PY_VERSION_HEX >= 0x030D0000
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL}
};

static int
_memoryboard_traverse(PyObject *module, visitproc visit, void *arg)
{
    return 0;
}

static int
_memoryboard_clear(PyObject *module)
{
    return 0;
}

static struct PyModuleDef _memoryboard = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_memoryboard",
    .m_doc = module_doc,
    .m_size = 0,
    .m_methods = _memoryboard_methods,
    .m_slots = _memoryboard_slots,
    .m_traverse = _memoryboard_traverse,
    .m_clear = _memoryboard_clear,
};


PyMODINIT_FUNC
PyInit__memoryboard(void)
{
    PyObject *m = PyModuleDef_Init(&_memoryboard);
    return m;
}
