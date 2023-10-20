import sys
import gc

#from https://stackoverflow.com/questions/13530762/how-to-know-bytes-size-of-python-object-like-arrays-and-dictionaries-the-simp
def get_obj_size(obj):
    marked = {id(obj)}
    obj_q = [obj]
    sz = 0

    while obj_q:
        sz += sum(map(sys.getsizeof, obj_q))

        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

        obj_q = new_refr.values()
        marked.update(new_refr.keys())

    return sz
