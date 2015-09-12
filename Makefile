
mdbCtest:
	libtool --mode=compile --tag=CC gcc -c `env PKG_CONFIG_PATH=/usr/lib/pkgconfig pkg-config --cflags monetdb-mapi` -lm mdbCtest.c
	libtool --mode=link --tag=CC gcc -o mdbCtest `env PKG_CONFIG_PATH=/usr/lib/pkgconfig pkg-config --libs monetdb-mapi` mdbCtest.o

mdbC:
	libtool --mode=compile --tag=CC gcc -c `env PKG_CONFIG_PATH=/usr/lib/pkgconfig pkg-config --cflags monetdb-mapi` mdbC.c
	libtool --mode=link --tag=CC gcc -o mdbC `env PKG_CONFIG_PATH=/usr/lib/pkgconfig pkg-config --libs monetdb-mapi` -lm mdbC.o


CPPFLAGS += -I/usr/include/postgresql

CFLAGS   += -g

LDFLAGS  += -g

LDLIBS   += -L/usr/include/postgresql/lib -lpq -lm

pgC:  pgC.o