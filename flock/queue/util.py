def split_options(s):
    if s == None:
        return []
    s = s.strip()
    if len(s) == 0:
        return []
    else:
        return s.split(" ")

def divide_into_batches(elements, size):
    for i in range(0, len(elements), size):
        yield elements[i:i + size]
