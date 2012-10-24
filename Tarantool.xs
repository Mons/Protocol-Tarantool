#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <tarantool/tnt.h>
#include <string.h>

#ifdef HAS_QUAD
#ifndef I64
typedef I64TYPE I64;
#endif
#ifndef U64
typedef U64TYPE U64;
#endif
#endif

#ifdef HAS_QUAD
#define HAS_LL 1
#define dUINTtypes \
	U64 int64;\
	U32 int32;\
	U16 int16;\
	U8  int8
#else
#define HAS_LL 0
#define dUINTtypes \
	unsigned long long int64 \
	U32 int32;\
	U16 int16;\
	U8  int8
#endif

//#define DEBUG_OVERALLOC
#undef DEBUG_OVERALLOC

unsigned char allowed_format[256] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 
	1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};


typedef struct {
	char   *f;
	size_t size;
} unpack_format;

typedef struct {
	struct tnt_stream s;
	struct tnt_stream_buf b;
	SV * sv;
	uint32_t cardinality;
} sv_buffer;

#define call_func_with_format(func, format, sv, ... )      \
	STMT_START {                                           \
		dUINTtypes;                                        \
		STRLEN __my_size;                                  \
		char  *__my_data;                                  \
		switch( format ) {                                 \
			case 'l':                                      \
				int64 = (U64) SvIV( sv );                  \
				func( __VA_ARGS__, (char * ) &int64, sizeof( U64 ) );        \
				break;                                     \
			case 'L':                                      \
				int64 = (U64) SvUV( sv );                  \
				func( __VA_ARGS__, (char * ) &int64, sizeof( U64 ) );        \
				break;                                     \
			case 'i':                                      \
				int32 = (U32) SvIV( sv );                  \
				func( __VA_ARGS__, (char * ) &int32, sizeof( U32 ) );        \
				break;                                     \
			case 'I':                                      \
				int32 = (U32) SvUV( sv );                  \
				func( __VA_ARGS__, (char * ) &int32, sizeof( U32 ) );        \
				break;                                     \
			case 's':                                      \
				int16 = (U16) SvIV( sv );                  \
				func( __VA_ARGS__, (char * ) &int16, sizeof( U16 ) );        \
				break;                                     \
			case 'S':                                      \
				int16 = (U16) SvUV( sv );                  \
				func( __VA_ARGS__, (char * ) &int16, sizeof( U16 ) );        \
				break;                                     \
			case 'c':                                      \
				int8 = (U8) SvIV( sv );                    \
				func( __VA_ARGS__, (char * ) &int8, sizeof( U8 ) );          \
				break;                                     \
			case 'C':                                      \
				int8 = (U8) SvUV( sv );                    \
				func( __VA_ARGS__, (char * ) &int8, sizeof( U8 ) );          \
				break;                                     \
			case 'p':                                      \
			case 'u':                                      \
				__my_data = SvPVbyte( (sv), __my_size );   \
				func( __VA_ARGS__, __my_data, __my_size ); \
				break;                                     \
			default:                                       \
				func( __VA_ARGS__, "", 0 );                \
				croak("Unsupported format: %s",format);    \
		}                                                  \
	} STMT_END
	

static void
tmake_tuple_pack( struct tnt_tuple *r, AV *t, unpack_format * format ) {
	int i;
	//struct tnt_tuple *r = tnt_mem_alloc( sizeof( struct tnt_tuple ) );
	//if ( !r ) croak("Can not allocate memory");
	tnt_tuple_init( r );
	
	dUINTtypes;
	char *data, *ptr, *end;
	// Every field needs enc of size. Max size of enc is 5 bytes. And reserve something for data. For ex 59 bytes up to 64
	if (av_len(t) == -1) return;
#ifdef DEBUG_OVERALLOC
	int mprof =
#endif
	r->size = ( av_len(t) + 1 ) * ( 64 );
	//warn ("r->size=%d", r->size);
	
	SV *buf = sv_2mortal( newSV( 4 + r->size  ) );
	sv_setpvn( buf, "\0\0\0\0", 4 ); // default cardinality to shift sv pos
	
	for (i = 0; i <= av_len( t ); i++) {
		STRLEN size;
		SV **val = av_fetch( t, i, 0 );
		if (!val || !SvOK(*val)) {
			data = "";
			size = 0;
			//tnt_tuple_add( r, "", 0 );
		} else {
			if (format && i < format->size) {
					switch( format->f[ i ] ) {
#ifdef HAS_QUAD
						case 'l':
							int64 = (U64) SvIV( *val );
							data = (char *) &int64;
							size = sizeof( U64 );
							//tnt_tuple_add( r, &int64, sizeof( U64 ) );
							break;
						case 'L':
							int64 = (U64) SvUV( *val );
							data = (char *) &int64;
							size = sizeof( U64 );
							//tnt_tuple_add( r, &int64, sizeof( U64 ) );
							break;
#endif
						case 'i':
							int32 = (U32) SvIV( *val );
							data = (char *) &int32;
							size = sizeof( U32 );
							//tnt_tuple_add( r, &int32, sizeof( U32 ) );
							break;
						case 'I':
							int32 = (U32) SvUV( *val );
							data = (char *) &int32;
							size = sizeof( U32 );
							//tnt_tuple_add( r, &int32, sizeof( U32 ) );
							break;
						case 's':
							int16 = (U16) SvIV( *val );
							data = (char *) &int16;
							size = sizeof( U16 );
							//tnt_tuple_add( r, &int16, sizeof( U16 ) );
							break;
						case 'S':
							int16 = (U16) SvUV( *val );
							data = (char *) &int16;
							size = sizeof( U16 );
							//tnt_tuple_add( r, &int16, sizeof( U16 ) );
							break;
						case 'c':
							int8 = (U8) SvIV( *val );
							data = (char *) &int8;
							size = sizeof( U8 );
							//tnt_tuple_add( r, &int8, sizeof( U8 ) );
							break;
						case 'C':
							int8 = (U8) SvUV( *val );
							data = (char *) &int8;
							size = sizeof( U8 );
							//tnt_tuple_add( r, &int8, sizeof( U8 ) );
							break;
						case 'p':
						case 'u':
							//SvUTF8_off(*val);
							//data = SvPVbytex( *val, size );
							data = SvPV( *val, size );
							break;
						default:
							croak("Unsupported format: %s",format->f[ i ]);
					}
			} else { // no format
				//SvUTF8_off(*val);
				//data = SvPVbyte( *val, size );
				data = SvPV( *val, size );
				//tnt_tuple_add( r, data, size );
			}
		}
		
		///Replacing...
		//tnt_tuple_add( r, data, size );
		
		r->cardinality++;
		//warn("Call grow on sv %p (%p). cur=%d, new=%d", buf, SvPV_nolen(buf), SvCUR(buf),SvCUR(buf) + size + 5);
		r->size = SvCUR(buf) + size + 5;
		ptr = SvGROW( buf, r->size ); // 5 is for enc
		//warn("Grown: ptr=%p", ptr);
		ptr += SvCUR(buf);
		
		//warn("Seeked: ptr=%p", ptr);
		end = tnt_enc_write(ptr, size);
		//warn ("End=%p, Enc=%d", end, end - ptr);
		SvCUR_set(buf, SvCUR(buf) + ( end - ptr ));
		//warn("catpvn: %p of size %d", data,size);
		sv_catpvn( buf, data, size );
	}
	
	* ( (U32 *) SvPV_nolen( buf ) ) = htole32( r->cardinality );
	r->data = SvPV(buf, r->size);
	//warn("generated tuple {%d}, size=%d (svcur=%d)\n", r->cardinality, r->size, SvCUR(buf));
#ifdef DEBUG_OVERALLOC
	warn("Memory overallocate=%d", mprof - r->size);
#endif
	return;
	//return r;
}


static ssize_t sv_writev(struct tnt_stream *s, struct iovec *iov, int count) {
	sv_buffer * buf = (sv_buffer *) s;
	STRLEN size = SvCUR( buf->sv );
	int i;
	for (i = 0 ; i < count ; i++) size += iov[i].iov_len;
	//warn("Called sv_writev(%p). Growing to size=%d", buf->sv, size);
	SvGROW( buf->sv, size );
	for (i = 0 ; i < count ; i++) {
		//warn ("catpvn[%d] p=%p of size %d into SV %p", i, iov[i].iov_base, iov[i].iov_len, buf->sv);
		assert(iov[i].iov_base);
		sv_catpvn( buf->sv, iov[i].iov_base, iov[i].iov_len );
	}
	s->wrcnt++;
	
	return size;
}

/*
static struct tnt_stream * tmake_buf() {
	sv_buffer * buf = safemalloc( sizeof( sv_buffer ) );
	memset(buf,0,sizeof(sv_buffer));
	if (!buf) croak("OOM!");
	//buf->sv = newSVpvn( "",0 ); // minimal packet size
	buf->sv = newSV( 12 ); // minimal packet size
	warn("Created sv: %p (%p)", buf, buf->sv);
	buf->s.writev = sv_writev;
	return &buf->s;
}
*/


static void oplist( struct tnt_stream *b, AV *ops ) {
	int i;
	//struct tnt_stream *b = tmake_buf();
	for (i = 0; i <= av_len( ops ); i++) {
		SV **val = av_fetch( ops, i, 0 );
		if (!*val || !SvROK( *val ) || SvTYPE( SvRV(*val) ) != SVt_PVAV )
			croak("Wrong update operation format: %s", val ? SvPV_nolen(*val) : "undef");
		
		AV *aop = (AV *)SvRV(*val);
		
		if ( av_len( aop ) < 1 ) croak("Too short operation argument list");
		
		U32 fno = SvUV( *av_fetch( aop, 0, 0 ) );
		char *opname = SvPV_nolen( *av_fetch( aop, 1, 0 ) );
		
		STRLEN size;
		U8     opcode = 0;
		
		switch (*opname) {
			case '#': //delete
				tnt_update_delete( b, fno );
				break;
			case '=': //set
				if ( av_len( aop ) < 3 ) croak("Too short operation argument list for %c. Need 3, have %d", *opname, av_len( aop ) );
				//char *data   = SvPV( *av_fetch( aop, 2, 0 ), size );
				
				call_func_with_format(tnt_update_assign, *SvPV_nolen( *av_fetch( aop, 3, 0 ) ), *av_fetch( aop, 2, 0 ), b, fno);
				//tnt_update_assign( b, fno, data, size );
				break;
			case '!': //insert
				if ( av_len( aop ) < 3 ) croak("Too short operation argument list for %c", *opname);
				call_func_with_format(tnt_update_insert, *SvPV_nolen( *av_fetch( aop, 3, 0 ) ), *av_fetch( aop, 2, 0 ), b, fno);
				break;
			case ':': //splice
				if ( av_len( aop ) < 3 ) croak("Too short operation argument list for %c", *opname);
				U32 offset = SvUV( *av_fetch( aop, 2, 0 ) );
				U32 length = SvUV( *av_fetch( aop, 3, 0 ) );
				char * data;
				if ( av_len( aop ) > 3 && SvOK( *av_fetch( aop, 4, 0 ) ) ) {
					data = SvPV( *av_fetch( aop, 4, 0 ), size );
				} else {
					data = "";
					size = 0;
				}
				tnt_update_splice( b, fno, offset, length, data, size );
				break;
			case '+': //add
				opcode = TNT_UPDATE_ADD;
				break;
			/* Bullshit. Client misses -
			case '-': //subtract
				opcode = TNT_UPDATE_ADD;
				break;
			*/
			case '&': //and
				opcode = TNT_UPDATE_AND;
				break;
			case '|': //or
				opcode = TNT_UPDATE_OR;
				break;
			case '^': //xor
				opcode = TNT_UPDATE_XOR;
				break;
			default:
				croak("Unknown operation: %c", *opname);
		}
		if (opcode) { // Arith ops
			if ( av_len( aop ) < 2 ) croak("Too short operation argument list for %c", *opname);
			unsigned long long v = SvUV( *av_fetch( aop, 2, 0 ) );
			tnt_update_arith( b, fno, opcode, v );
		}
	}
	//return b;
}

static AV * extract_tuples(struct tnt_reply *r, unpack_format * format, char default_string) {
	struct tnt_iter it;
	tnt_iter_list(&it, TNT_REPLY_LIST(r));
	AV *res = newAV();
	sv_2mortal((SV *)res);

	while (tnt_next(&it)) {
		struct tnt_iter ifl;
		struct tnt_tuple *tu = TNT_ILIST_TUPLE(&it);
		tnt_iter(&ifl, tu);
		AV *t = newAV();
		int idx = 0;
		while (tnt_next(&ifl)) {
			char    *data = TNT_IFIELD_DATA(&ifl);
			uint32_t size = TNT_IFIELD_SIZE(&ifl);
			if (format && idx < format->size) {
					switch( format->f[ idx ] ) {
#ifdef HAS_QUAD
						case 'l':
							if (size != 8) warn("Field l should be of size 8, but got: %d", size);
							av_push( t, newSViv( le64toh( *( I64 *) data ) ) );
							break;
						case 'L':
							if (size != 8) warn("Field L should be of size 8, but got: %d", size);
							av_push( t, newSVuv( le64toh( *( U64 *) data ) ) );
							break;
#endif
						case 'i':
							if (size != 4) warn("Field i should be of size 4, but got: %d", size);
							av_push( t, newSViv( le32toh( *( I32 *) data ) ) );
							break;
						case 'I':
							if (size != 4) warn("Field I should be of size 4, but got: %d", size);
							//warn( "I32: %lu (%02x %02x %02x %02x)", * ( I32 * ) data, *data, *(data+1), 0,0 );
							av_push( t, newSVuv( le32toh( *( U32 *) data ) ) );
							break;
						case 's':
							if (size != 2) warn("Field s should be of size 2, but got: %d", size);
							av_push( t, newSViv( le16toh( *( I16 *) data ) ) );
							break;
						case 'S':
							if (size != 2) warn("Field S should be of size 2, but got: %d", size);
							av_push( t, newSVuv( le16toh( *( U16 *) data ) ) );
							break;
						case 'c':
							if (size != 1) warn("Field c should be of size 1, but got: %d", size);
							av_push( t, newSViv( *( I8 *) data ) );
							break;
						case 'C':
							if (size != 1) warn("Field C should be of size 1, but got: %d", size);
							av_push( t, newSVuv( *( U8 *) data ) );
							break;
						case 'p':
							av_push(t, newSVpvn_utf8(data, size, 0));
							break;
						case 'u':
							av_push(t, newSVpvn_utf8(data, size, 1));
							break;
						default:
							croak("Unsupported format: %s",format->f[ idx ]);
					}
			} else { // no format
				if (default_string == 'u') {
					av_push(t, newSVpvn_utf8(data, size, 1));
				} else {
					av_push(t, newSVpvn_utf8(data, size, 0));
				}
			}
			idx++;
		}
		av_push(res, newRV_noinc((SV *) t));
	}
	return res;
}

#define SV_BUFFER(var, size, id) \
	STMT_START { \
		memset(&var,0,sizeof(sv_buffer)); \
		var.sv = newSV( size ); \
		sv_setpvn( var.sv, "", 0 ); \
		var.s.writev = sv_writev; \
		var.s.reqid = id; \
		var.s.data = &var.b; \
	} STMT_END

#define CHECK_PACK_FORMAT(src) \
	STMT_START { \
				char *p = src;\
				while(*p) { \
					switch(*p) { \
						case 'l':case 'L': \
							if (!HAS_LL) { croak("Int64 support was not compiled in"); break; } \
						case 'i':case 'I': \
						case 's':case 'S': \
						case 'c':case 'C': \
						case 'p':case 'u': \
							p++; break; \
						default: \
							croak("Unknown pattern in format: %c", *p); \
					} \
				} \
	} STMT_END


MODULE = Protocol::Tarantool		PACKAGE = Protocol::Tarantool
PROTOTYPES: ENABLE

BOOT:
	HV *stash = gv_stashpv("Protocol::Tarantool", TRUE);
	newCONSTSUB(stash, "TNT_INSERT", newSViv(TNT_OP_INSERT));
	newCONSTSUB(stash, "TNT_SELECT", newSViv(TNT_OP_SELECT));
	newCONSTSUB(stash, "TNT_UPDATE", newSViv(TNT_OP_UPDATE));
	newCONSTSUB(stash, "TNT_DELETE", newSViv(TNT_OP_DELETE));
	newCONSTSUB(stash, "TNT_CALL",   newSViv(TNT_OP_CALL));
	newCONSTSUB(stash, "TNT_PING",   newSViv(TNT_OP_PING));
	newCONSTSUB(stash, "TNT_FLAG_RETURN", newSViv(TNT_FLAG_RETURN));
	newCONSTSUB(stash, "TNT_FLAG_ADD", newSViv(TNT_FLAG_ADD));
	newCONSTSUB(stash, "TNT_FLAG_REPLACE", newSViv(TNT_FLAG_REPLACE));
	newCONSTSUB(stash, "TNT_FLAG_BOX_QUIET", newSViv(TNT_FLAG_BOX_QUIET));
	newCONSTSUB(stash, "TNT_FLAG_NOT_STORE", newSViv(TNT_FLAG_NOT_STORE));
	tnt_mem_init(saferealloc);

void * test ()
	CODE:
		I32  nt = 0x01234567;
		char be[8] = "\0\001\002\003\004\005\006\007";
		//warn( "%08x", be32toh( (unsigned int )be ) );

SV * ping( req_id )
	U32 req_id

	PROTOTYPE: $
	CODE:
		sv_buffer buf;
		SV_BUFFER(buf, 12, req_id);
		RETVAL = buf.sv;
		
		tnt_ping( &buf.s );
		
		/*
		// For fun. Portability is more important
		char x[12] = {
			0, 0xff, 0, 0,
			0, 0, 0, 0,
			( req_id & 0xff ),
			( (req_id >> 8) & 0xff ),
			( (req_id >> 16) & 0xff ),
			( (req_id >> 24) & 0xff )
		};
		RETVAL = newSVpvn( x,12 );
		*/
	OUTPUT:
		RETVAL

SV * select( req_id, ns, idx, offset, limit, keys, ... )
	U32 req_id
	U32 ns
	U32 idx
	U32 offset
	U32 limit
	AV *keys

	PROTOTYPE: $$$$$$;$
	CODE:
		int i;
		struct tnt_list list;
		struct tnt_tuple *r, *rhead = 0;
		tnt_list_init( &list );
		
		unpack_format format;
		format.f = "";
		format.size = 0;
		if (items > 6) {
			if ( SvOK(ST(6)) && SvPOK(ST(6)) ) {
				format.f = SvPVbyte(ST(6), format.size);
				CHECK_PACK_FORMAT( format.f );
			}
			else if (!SvOK( ST(6) )) {}
			else {
				croak("Usage: select( req_id, ns, idx, offset, limit, keys [ ,format_string ] )");
			}
			
		}
		//warn("select(req=%u, ns=%u, idx=%u, oft=%u, lim=%u, keys=(AV *)%p, fmt=%s)", req_id, ns, idx, offset, limit, keys, format.f);

		if ( ( list.count = av_len ( keys ) + 1 ) ) {
			list.list = safemalloc(sizeof( struct tnt_list_ptr ) * list.count);
			rhead = r = safemalloc(sizeof( struct tnt_tuple ) * list.count);
			//tnt_mem_alloc(
			//	sizeof( struct tnt_list_ptr ) * list.count
			//);
			
			if ( !list.list ) croak("Can't allocate memory");
			for (i = 0; i < list.count; i++) {
				SV *t = *av_fetch( keys, i, 0 );
				if (!SvROK(t) || (SvTYPE(SvRV(t)) != SVt_PVAV)) croak("keys must be ARRAYREF of ARRAYREF");
				
				tmake_tuple_pack( r, (AV *)SvRV(t), &format );
				list.list[i].ptr = r;
				r++;
			}
		}
		
		sv_buffer buf;
		SV_BUFFER(buf, 64, req_id); // select header is 32 bytes. give prealloc for at least 64
		RETVAL = buf.sv;
		tnt_select( &buf.s, ns, idx, offset, limit, &list );
		if (list.count) {
			safefree( list.list );
			safefree( rhead );
		}
		//tnt_list_free( &list );

	OUTPUT:
		RETVAL


SV * insert( req_id, ns, flags, tuple, ... )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV * tuple

	PROTOTYPE: $$$$;$
	CODE:
		unpack_format format; format.f = ""; format.size = 0;
		if (items > 4 ) {
			if ( SvPOK( ST(4) ) ) {
				format.f = SvPVbyte(ST(4), format.size);
				CHECK_PACK_FORMAT(format.f);
				//warn("Used format for insert [%lu]: %s", format.size, format.f);
			}
			else if (!SvOK( ST(4) )) {}
			else {
				croak("Usage: insert( req_id, ns, flags, tuple [, format_string ] )");
			}
		}
		
		sv_buffer buf;
		SV_BUFFER(buf, 64, req_id); // insert header is 24 bytes. give prealloc for at least 64
		RETVAL = buf.sv;
		struct tnt_tuple r;
		
		//struct tnt_tuple *t = 
		tmake_tuple_pack( &r, tuple, &format );
		tnt_insert( &buf.s, ns, flags, &r );
		//tnt_tuple_free( t );
		
	OUTPUT:
		RETVAL

SV * update( req_id, ns, flags, tuple, operations, ... )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV *tuple
	AV *operations

	PROTOTYPE: $$$$$;$
	CODE:
		unpack_format format; format.f = ""; format.size = 0;
		if (items > 5 ) {
			if ( SvPOK( ST(5) ) ) {
				format.f = SvPVbyte(ST(5), format.size);
				CHECK_PACK_FORMAT(format.f);
			}
			else if (!SvOK( ST(5) )) {}
			else {
				croak("Usage: update( req_id, ns, flags, tuple, operations [, format_string ] )");
			}
		}
		
		sv_buffer buf;
		SV_BUFFER(buf, 64, req_id); // update header is 24 bytes. give prealloc for at least 64
		RETVAL = buf.sv;
		
		sv_buffer ops;
		SV_BUFFER(ops, 64, 0); // preallocate some
		sv_2mortal(ops.sv);
		
		struct tnt_tuple r;
		tmake_tuple_pack( &r, tuple, &format );
		oplist( &ops.s, operations );
		ops.b.data = SvPV( ops.sv, ops.b.size );
		
		tnt_update( &buf.s, ns, flags, &r, &ops.s );
		
		//tnt_tuple_free( t );
		//tnt_stream_free( ops );
		
	OUTPUT:
		RETVAL

SV * delete( req_id, ns, flags, tuple, ... )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV *tuple

	PROTOTYPE: $$$$;$
	CODE:
	
		unpack_format format; format.f = ""; format.size = 0;
		
		if (items > 4 ) {
			if ( SvPOK( ST(4) ) ) {
				format.f = SvPVbyte(ST(4), format.size);
				CHECK_PACK_FORMAT(format.f);
			}
			else if (!SvOK( ST(4) )) {}
			else {
				croak("Usage: delete( req_id, ns, flags, tuple [, format_string ] )");
			}
		}
		
		sv_buffer buf;
		SV_BUFFER(buf, 32, req_id); // delete header is 24 bytes. but likely it will contain no data except key. give prealloc for 32 (will be enough for one int64)
		RETVAL = buf.sv;
		
		struct tnt_tuple r;
		tmake_tuple_pack( &r, tuple,&format );
		tnt_delete( &buf.s, ns, flags, &r );
		//tnt_tuple_free( t );
	OUTPUT:
		RETVAL

SV * lua( req_id, flags, proc, tuple, ... )
	unsigned req_id
	unsigned flags
	char *proc
	AV *tuple

	PROTOTYPE: $$$$;$
	CODE:
		unpack_format format; format.f = ""; format.size = 0;
		if (items > 4 ) {
			if ( SvPOK( ST(4) ) ) {
				format.f = SvPVbyte(ST(4), format.size);
				CHECK_PACK_FORMAT(format.f);
			}
			else if (!SvOK( ST(4) )) {}
			else {
				croak("Usage: lua( req_id, flags, proc, tuple [, format_string ] )");
			}
		}
		sv_buffer buf;
		SV_BUFFER(buf, 64, req_id); // lua header is 20 bytes. give prealloc for 64.
		RETVAL = buf.sv;
		
		struct tnt_tuple r;
		//warn("call tuple pack");
		tmake_tuple_pack( &r, tuple, &format );
		//warn("call tnt_call");
		tnt_call( &buf.s, flags, proc, &r );
		//tnt_tuple_free( t );
	OUTPUT:
		RETVAL



HV * response( response, ... )
	SV *response

	PROTOTYPE: $;@
	INIT:
		RETVAL = newHV();
		sv_2mortal((SV *)RETVAL);
	CODE:
		if ( !SvOK(response) )
			croak( "response is undefined: %s", SvPV_nolen(response) );
		if ( SvROK(response) ) {
			switch (SvTYPE( SvRV(response) )) {
				case SVt_PV:
				case SVt_PVLV:
					response = SvRV(response);
					break;
				default:
					croak("Svtype: %d", SvTYPE( SvRV(response) ));
			}
		}
			
		unpack_format format;
		format.f = "";
		format.size = 0;
		char default_string = 'p';
		
		if (items > 1) {
			if ( SvPOK(ST(1)) ) {
				char *p = format.f = SvPVbyte(ST(1), format.size);
				while(*p) {
/*
                                       switch(*p) {
#ifdef HAS_QUAD
                                               case 'l':
                                               case 'L':
#endif
                                               case 'i':
                                               case 'I':
                                               case 's':
                                               case 'S':
                                               case 'p':
                                               case 'u':
                                                       p++;
                                                       break;
                                               default:
                                                       croak("Unknown pattern in format: %c", *p);
                                       }
*/
					if (!allowed_format[ *p++ ])
						croak("Unknown pattern in format: %c", *p);
				}
				//warn("Used format [%lu]: %s", format.size, format.f);
			}
			else if (!SvOK(ST(1))) {
				//
			}
			else {
				croak("Bad format string. Usage: response(packet [ , format_string [, default_string=[p|u] ] ])");
			}
			if ( items > 2 && SvPOK(ST(2)) ) {
				STRLEN l;
				char * p = SvPV( ST(2), l );
				if (l == 1 && ( *p == 'p' || *p == 'u' )) {
					default_string=*p;
				} else {
					croak("Bad default_string: %s (%d)", p, l);
				}
			}
		}
		STRLEN size;
		char *data = SvPV( response, size );
		struct tnt_reply reply;
		
		
		tnt_reply_init( &reply );
		size_t offset = 0;
		int cnt = tnt_reply( &reply, data, size, &offset );
		
		if ( cnt != 0 ) {
			if (cnt < 0) {
				(void) hv_stores(RETVAL, "status", newSVpvs("fatal"));
				(void) hv_stores(RETVAL, "errstr", newSVpvs("Can't parse server response"));
			} else {
				(void) hv_stores(RETVAL, "status", newSVpvs("buffer"));
				(void) hv_stores(RETVAL, "errstr", newSVpvs("Input data too short"));
			}
			if (reply.code)
				(void) hv_stores(RETVAL, "code",   newSViv(reply.code));
			if (reply.reqid)
				(void) hv_stores(RETVAL, "id",   newSViv(reply.reqid));
			if (reply.op)
				(void) hv_stores(RETVAL, "op",   newSViv(reply.op));
		} else
		{
			(void) hv_stores(RETVAL, "code",    newSViv(reply.code));
			(void) hv_stores(RETVAL, "id",      newSViv(reply.reqid));
			(void) hv_stores(RETVAL, "op",      newSViv(reply.op));
			(void) hv_stores(RETVAL, "count",   newSViv(reply.count));
			if (reply.code) {
				(void) hv_stores(RETVAL, "status", newSVpvs("error"));
				(void) hv_stores(RETVAL, "errstr", newSVpv(reply.error,0));
			} else {
				(void) hv_stores(RETVAL, "status", newSVpvs("ok"));
				AV *tuples = extract_tuples( &reply, &format, default_string );
				(void) hv_stores(RETVAL, "tuples", newRV((SV *)tuples));
			}
		}
		tnt_reply_free( &reply );
	
	OUTPUT:
		RETVAL

