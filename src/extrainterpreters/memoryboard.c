#include <Python.h>

PyDoc_STRVAR(_memoryboard_remote_memory_doc,
"remote_memory(buffer_address, buffer_length)\n\
\n\
For internal extrainterpreters uses only \
Returns a writable memoryview object pointing to the\
the indicated memory. \n\
\n\
THIS IS UNSAFE AND WILL CRASH THE PROCESS IF USED INCORRECTLY.\
");


static PyObject *_memoryboard_remote_memory(PyObject *self, PyObject *args)
{
    unsigned long size;
    Py_ssize_t tmp;
    char *memory;

    if (!PyArg_ParseTuple(args, "nk", &tmp, &size)) {
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
    PyObject buffer_address, size;

    if (!PyArg_ParseTuple(args, "y*", &buffer)) {
        return NULL;
    }

    return Py_BuildValue(
      "(OO)",
     PyLong_FromSsize_t((Py_ssize_t)buffer.buf),
     PyLong_FromLong(buffer.len)
    );
}

static PyMethodDef _memoryboard_methods[] = {
    {"remote_memory", _memoryboard_remote_memory, METH_VARARGS, _memoryboard_remote_memory_doc},
    {"address_and_size", _memoryboard_get_address_and_size, METH_VARARGS, _memoryboard_get_address_and_size_doc},
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
    return PyModuleDef_Init(&_memoryboard);
}
