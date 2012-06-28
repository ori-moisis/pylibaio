#include <Python.h>
#include <libaio.h>

static const int MAX_EVENT = 128;
static io_context_t io_ctx = 0;


PyDoc_STRVAR (write_doc_str, 
              "write(fd, offsets_and_buffers)\n"
              "  offsets_and_buffers is a list of tuples, each one with an offset and a buffer to write to that offset");

PyObject*
pyaio_write (PyObject* dummy, PyObject* args) {
    int fd = 0;
    PyObject* offsets_and_bufs;

    PyArg_ParseTuple(args, "iO", &fd, &offsets_and_bufs);
    
    if (PyList_Check(offsets_and_bufs) == 0) {
        return Py_None;
    }

    long lst_len = PyList_GET_SIZE(offsets_and_bufs);
    iocb** iocbs = new iocb*[lst_len];

    for (long i = 0; i < lst_len; ++i) {
        iocb* cb = new iocb;
        iocbs[i] = cb;

        // Parse element
        PyObject* elem = PyList_GET_ITEM (offsets_and_bufs, i);
        PyObject* offset_obj = PyTuple_GET_ITEM(elem, 0);
        PyObject* buf_obj = PyTuple_GET_ITEM(elem, 1);

        // Copy buffer
        char* buf = PyString_AS_STRING (buf_obj);
        long buf_size = PyString_GET_SIZE(buf_obj);

        char* copied_buf = new char[buf_size];
        memcpy(copied_buf, buf, buf_size);

        // Parse offset
        long offset = PyInt_AS_LONG(offset_obj);

        io_prep_pwrite (cb, fd, copied_buf, buf_size, offset);
    }

    int ret = io_submit(io_ctx, lst_len, iocbs);

    delete [] iocbs;

    return Py_BuildValue ("i", ret);
}

PyDoc_STRVAR (read_doc_str, "read(fd, offsets_and_counts)");

PyObject*
pyaio_read (PyObject* dummy, PyObject* args) {
    int fd = 0;
    PyObject* offsets_and_counts;

    PyArg_ParseTuple(args, "iO", &fd, &offsets_and_counts);

    if (PyList_Check(offsets_and_counts) == 0) {
        return Py_None;
    }

    long lst_len = PyList_GET_SIZE(offsets_and_counts);
    iocb** iocbs = new iocb*[lst_len];

    for (long i = 0; i < lst_len; ++i) {
        iocb* cb = new iocb;
        iocbs[i] = cb;

        // Parse element
        PyObject* elem = PyList_GET_ITEM (offsets_and_counts, i);
        PyObject* offset_obj = PyTuple_GET_ITEM(elem, 0);
        PyObject* count_obj = PyTuple_GET_ITEM(elem, 1);

        long offset = PyInt_AS_LONG(offset_obj);
        long cnt = PyInt_AS_LONG(count_obj);

        char* buf = new char[cnt];
        memset(buf, 0, cnt);

        io_prep_pread (cb, fd, buf, cnt, offset);
    }

    int ret = io_submit(io_ctx, lst_len, iocbs);

    delete [] iocbs;

    return Py_BuildValue ("i", ret);
}

PyDoc_STRVAR (get_events_doc_str, "get_events(min_events, max_events) -> [events]");

PyObject*
pyaio_get_events (PyObject* dummy, PyObject* args) {
    int max_events = 0;
    int min_events = 0;

    PyArg_ParseTuple (args, "ii", &min_events, &max_events);

    io_event* events = new io_event[max_events];
    int res = io_getevents (io_ctx, min_events, max_events, events, NULL);
    if (res == 0) {
        delete events;
        return Py_None;
    }

    PyObject* ret = PyList_New (0);

    for (int i = 0; i < res; ++i) {
        io_event& eve = events[i];
        iocb* cb = eve.obj;
        PyList_Append(ret, Py_BuildValue("(iiO)", cb->aio_lio_opcode, cb->u.c.offset, PyString_FromStringAndSize ((char*)cb->u.c.buf, cb->u.c.nbytes)));
        delete [] (char*)cb->u.c.buf;
        delete cb;
    }

    delete [] events;
    
    return ret;
}

static PyMethodDef module_methods[] = {

        { "write", pyaio_write,
        METH_VARARGS, write_doc_str },

        { "read", pyaio_read,
        METH_VARARGS, read_doc_str },

        { "get_events", pyaio_get_events,
        METH_VARARGS, get_events_doc_str },

        { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC
initpylibaio () {
    Py_InitModule ("pylibaio", module_methods);
    // Initialize context
    io_setup(MAX_EVENT, &io_ctx);
}
