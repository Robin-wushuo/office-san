import array
from struct import unpack
from collections import namedtuple


f = open('/users/san/hello.pdf', 'rb')
v = memoryview(f.read())
assert f.tell() == f.seek(0, 2)
total = f.seek(0, 2)
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
# between two explicit byte strings
xrefoffset = int(c[c.index(b'startxref\n')+10:c.index(b'\n%%EOF')])
trailerend = int(c.index(b'startxref\n')) - 30 + total

# ... and create cross reference table(xref)
c = peekb(v, xrefoffset + 5, xrefoffset + 15)
clist = c.replace(b'\n', b' ', 1).split(maxsplit=2)
objectcount, countfrom = int(clist[1]), int(clist[0])
objstart = xrefoffset + 5 + c.index(b'\n') + 1
objtable = array.array('H') # Catalog index is 1
# assume object revision number is 0
while countfrom < objectcount:
    objoffset = int(peekb(v, objstart, objstart + 10))
    objtable.append(objoffset)
    objstart += 20
    countfrom += 1

# trailer contains root, i.e. catalog
trailerstart = objstart # trailerend have been calculated

def peekr(cont, start, end):
    "Find object reference recursivly, just twice maybe"
    assert end > start
    startn = start
    amount = 0
    while amount < end - start:
        amount += 10 * len(cont)
        c = peekb(v, startn, startn + 10 * len(cont))
        if cont in c:
            i = c.index(cont)
            newstart = startn + i + len(cont) + 1
            c = peekb(v, newstart, newstart + 50) # 50 not enough for Kids
            return c
        startn += 10 * len(cont)
    peekr(cont, start + len(cont), end)

Objrange = namedtuple('Objrange', 'start end')
default_objrange = Objrange(0, 0)
offsets = sorted(objtable)

def peekrx(cont, start, end, maxsplit=-1, index=0):
    c = peekr(cont, start, end)
    i = int(c.split(maxsplit=maxsplit)[index])
    r = default_objrange._replace(start=objtable[i])
    if objtable[i] != offsets[-1]:
        r = r._replace(end = offsets[offsets.index(objtable[i]) + 1])
    else:
        r = r._replace(end = xrefoffset)
    return r

root = peekrx(b'/Root', trailerstart, trailerend, maxsplit=1)
pages = peekrx(b'/Pages', root.start, root.end, maxsplit=1)
kid = peekrx(b'/Kids', pages.start, pages.end, maxsplit=2, index=1)
contents = peekrx(b'/Contents', kid.start, kid.end, maxsplit=1)
result = peekb(v, contents.start, contents.end)
f.close()
print(result)
