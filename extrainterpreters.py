def call_in_interpreter(inter, func, args=(), kwargs=None, thread=None):
    kwargs = kwargs or {}
    parcels = [pickle.dumps(item) for item in (func, args, kwargs)]
    indices = [len(parcels[0]), len(parcels[1])]
    bundle = b"".join(parcels)
    code = f"""import ctypes, pickle; bundle=ctypes.string_at({id(bundle) + sys.getsizeof(bundle) - len(bundle) - 1}, {len(bundle)} ); aliens = [pickle.loads(bundle[start:end]) for start, end in ((0, {indices[0]}), ({indices[0]}, {indices[1]}), ({indices[1]}, None))]; aliens[0](*aliens[1], **aliens[2])"""
    print(code)
    t = threading.Thread(target=interpreters.run_string, args=(inter, code))
    t.start()
    return t
