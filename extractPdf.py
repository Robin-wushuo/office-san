import array
import re
import zlib
import bisect
from struct import unpack
from collections import namedtuple


path = '/users/san/threepages.pdf'
f = open(path, 'rb')
v = memoryview(f.read())
assert f.tell() == f.seek(0, 2)
total = f.seek(0, 2)
ObjectRange = namedtuple('ObjectRange', 'start end')

# First, find xref ...


def peekb(buffer, start, end=None):
    "Peek a piece of memory in buffer"
    if end is None and start < 0:
        fmt = str(-start) + 's'
        content = unpack(fmt, buffer[start:])
    else:
        assert end > start
        fmt = str(end - start) + 's'
        content = unpack(fmt, buffer[start:end])
    return content[0]


# peek the end of pdf, 30 bytes maybe enough
c = peekb(v, -30)
newline = c.partition(b'%%EOF')[2]
newline_l = len(newline)
# between two explicit byte strings
xrefoffset_start = c.index(b'startxref') + 9 + newline_l
xrefoffset_end = c.index(newline + b'%%EOF')
xrefoffset = int(c[xrefoffset_start:xrefoffset_end])
trailer_end = c.index(b'startxref') - 30 + total

# ... and create cross reference table(xref)
c = peekb(v, xrefoffset + 4 + newline_l, xrefoffset + 15)
clist = c.replace(newline, b' ', 1).split(maxsplit=2)
objectcount, countfrom = int(clist[1]), int(clist[0])
objstart = xrefoffset + 4 + newline_l + c.index(newline) + newline_l
xref = array.array('H')
orderxref = array.array('H')
# assume: object revision number is 0
while countfrom < objectcount:
    objoffset = int(peekb(v, objstart, objstart + 10))
    xref.append(objoffset)
    bisect.insort(orderxref, objoffset)
    objstart += 19 + newline_l
    countfrom += 1

# trailer contains root, i.e. catalog
trailer_start = objstart # trailer_end have been calculated


def find_offset(cont, start, end):
    assert end > start
    startn = start
    amount = 0
    while amount < end - start:
        amount += 10 * len(cont)
        c = peekb(v, startn, startn + 10 * len(cont))
        if cont in c:
            i = c.index(cont)
            return startn + i
        startn += 10 * len(cont)
    find_offset(cont, start + len(cont), end)


def find_obj(cont, start, end):
    cont_at = find_offset(cont, start, end)
    obj_number_at = cont_at + len(cont)
    c = peekb(v, obj_number_at, obj_number_at + 50).lstrip()
    obj_number = int(c.partition(b' ')[0])
    return obj_number


def find_range(number):
    obj_start = xref[number]
    i = bisect.bisect_right(orderxref, obj_start)
    if i != len(orderxref):
        obj_end = orderxref[i]
    else:
        obj_end = xrefoffset
    return ObjectRange(obj_start, obj_end)


def find_kids(start, end):
    new_start = find_offset(b'/Kids', start, end)
    kids_start = find_offset(b'[', new_start, end)
    kids_end = find_offset(b']', new_start, end)
    c = peekb(v, kids_start, kids_end).strip(b'[]')
    kids_list = c.split(b' R')
    kids_list.pop()
    kid_number = (int(a.lstrip().partition(b' ')[0]) for a in kids_list)
    return kid_number


def extract(start, end):
    stream_start = find_offset(b'stream', start, end) + 6 + newline_l
    stream_end = find_offset(b'endstream', start, end) - newline_l
    stream = peekb(v, stream_start, stream_end)
    bit_string = zlib.decompress(stream)
    # use re to extract text
    iterator = re.finditer(rb'\( (.*?) \)', bit_string, re.S | re.X)
    bit_result = b''
    for match in iterator:
        bit_result += match.group(1)
    unicode_string = bit_result.decode()
    return unicode_string  


root = find_obj(b'/Root', trailer_start, trailer_end)
root_range = find_range(root)
pages = find_obj(b'/Pages', root_range.start, root_range.end)
pages_range = find_range(pages)
kids = find_kids(pages_range.start, pages_range.end)
# assume: kids are leaves, there is no node in kids
for kid in kids:
    kid_range = find_range(kid)
    cont = find_obj(b'/Contents', kid_range.start, kid_range.end)
    cont_range = find_range(cont)
    text = extract(cont_range.start, cont_range.end)
    print(text)
    
f.close()
