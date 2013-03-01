#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#define NEED_newCONSTSUB
#define NEED_newRV_noinc
#define NEED_newSVpvn_flags
#define NEED_sv_2pv_flags
#define NEED_sv_2pvbyte
#include "ppport.h"

#include <string.h>

#include <endian.h>
#ifndef le64toh
# include <byteswap.h>
# if __BYTE_ORDER == __LITTLE_ENDIAN

#ifndef le16toh
#  define htobe16(x) __bswap_16 (x)
#  define htole16(x) (x)
#  define be16toh(x) __bswap_16 (x)
#  define le16toh(x) (x)
#endif

#ifndef le32toh
#  define htobe32(x) __bswap_32 (x)
#  define htole32(x) (x)
#  define be32toh(x) __bswap_32 (x)
#  define le32toh(x) (x)
#endif

#ifndef le64toh
#  define htobe64(x) __bswap_64 (x)
#  define htole64(x) (x)
#  define be64toh(x) __bswap_64 (x)
#  define le64toh(x) (x)
#endif

# else

#ifndef le16toh
#  define htobe16(x) (x)
#  define htole16(x) __bswap_16 (x)
#  define be16toh(x) (x)
#  define le16toh(x) __bswap_16 (x)
#endif

#ifndef le32toh
#  define htobe32(x) (x)
#  define htole32(x) __bswap_32 (x)
#  define be32toh(x) (x)
#  define le32toh(x) __bswap_32 (x)
#endif

#ifndef le64toh
#  define htobe64(x) (x)
#  define htole64(x) __bswap_64 (x)
#  define be64toh(x) (x)
#  define le64toh(x) __bswap_64 (x)
#endif
# endif
#endif

#define TNT_OP_INSERT      13
#define TNT_OP_SELECT      17
#define TNT_OP_UPDATE      19
#define TNT_OP_DELETE      21
#define TNT_OP_CALL        22
#define TNT_OP_PING        65280

#define TNT_FLAG_RETURN    0x01
#define TNT_FLAG_ADD       0x02
#define TNT_FLAG_REPLACE   0x04
#define TNT_FLAG_BOX_QUIET 0x08
#define TNT_FLAG_NOT_STORE 0x10

enum {
	TNT_UPDATE_ASSIGN = 0,
	TNT_UPDATE_ADD,
	TNT_UPDATE_AND,
	TNT_UPDATE_XOR,
	TNT_UPDATE_OR,
	TNT_UPDATE_SPLICE,
	TNT_UPDATE_DELETE,
	TNT_UPDATE_INSERT,
};


#ifndef I64
typedef int64_t I64;
#endif

#ifndef U64
typedef uint64_t U64;
#endif

#ifdef HAS_QUAD
#define HAS_LL 1
#else
#define HAS_LL 0
#endif



typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
} tnt_hdr_t;

typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t code;
} tnt_res_t;

typedef struct {
	uint32_t ns;
	uint32_t flags;
} tnt_hdr_nsf_t;

typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t space;
	uint32_t flags;
} tnt_pkt_insert_t;

typedef tnt_pkt_insert_t tnt_pkt_delete_t;
typedef tnt_pkt_insert_t tnt_pkt_update_t;

typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t space;
	uint32_t index;
	uint32_t offset;
	uint32_t limit;
	uint32_t count;
} tnt_pkt_select_t;


typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t flags;
} tnt_pkt_call_t;


typedef
	union {
		char     *c;
		U32      *i;

		U64      *q;
		U16      *s;
	} uniptr;

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
	char    def;
	char   *f;
	size_t size;
} unpack_format;


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

#define dUnpackFormat(fvar) unpack_format fvar; fvar.f = ""; fvar.size = 0; fvar.def  = 'p'

#define dExtractFormat(fvar,pos,usage) STMT_START {                  \
		if (items > pos) {                                           \
			if ( SvOK(ST(pos)) && SvPOK(ST(pos)) ) {                 \
				fvar.f = SvPVbyte(ST(pos), fvar.size);               \
				CHECK_PACK_FORMAT( fvar.f );                         \
			}                                                        \
			else if (!SvOK( ST(pos) )) {}                            \
			else {                                                   \
				croak("Usage: " usage " [ ,format_string [, default_unpack ] ] )");   \
			}                                                        \
			if ( items > pos + 1 && SvPOK(ST( pos + 1 )) ) {         \
				STRLEN _l;                                           \
				char * _p = SvPV( ST( pos + 1 ), _l );               \
				if (_l == 1 && ( *_p == 'p' || *_p == 'u' )) {       \
					format.def = *_p;                                \
				} else {                                             \
					croak("Bad default: %s; Usage: " usage " [ ,format_string [, default_unpack ] ] )", _p);   \
				}                                                    \
			}                                                        \
			                                                         \
		}                                                            \
	} STMT_END





#define uptr_cat_sv_fmt( up, src, format )                                 \
	STMT_START {                                                           \
		switch( format ) {                                                 \
			case 'l': *( up.q ++ ) = htole64( (U64) SvIV( src ) ); break; \
			case 'L': *( up.q ++ ) = htole64( (U64) SvUV( src ) ); break; \
			case 'i': *( up.i ++ ) = htole32( (U32) SvIV( src ) ); break; \
			case 'I': *( up.i ++ ) = htole32( (U32) SvUV( src ) ); break; \
			case 's': *( up.s ++ ) = htole16( (U16) SvIV( src ) ); break; \
			case 'S': *( up.s ++ ) = htole16( (U16) SvUV( src ) ); break; \
			case 'c': *( up.c ++ ) = (U8) SvIV( src ); break;             \
			case 'C': *( up.c ++ ) = (U8) SvUV( src ); break;             \
			case 'p': case 'u':                                           \
				memcpy( up.c, SvPV_nolen(src), sv_len(src)  );        \
				up.c += sv_len(src);                                      \
				break;                                                    \
			default:                                       \
				croak("Unsupported format: %s",format);    \
		}                                                  \
	} STMT_END

#define uptr_field_sv_fmt( up, src, format )                               \
	STMT_START {                                                           \
		switch( format ) {                                                 \
			case 'l': *(up.c++) = 8; *( up.q++ ) = htole64( (U64) SvIV( src ) ); break; \
			case 'L': *(up.c++) = 8; *( up.q++ ) = htole64( (U64) SvUV( src ) ); break; \
			case 'i': *(up.c++) = 4; *( up.i++ ) = htole32( (U32) SvIV( src ) ); break; \
			case 'I': *(up.c++) = 4; *( up.i++ ) = htole32( (U32) SvUV( src ) ); break; \
			case 's': *(up.c++) = 2; *( up.s++ ) = htole16( (U16) SvIV( src ) ); break; \
			case 'S': *(up.c++) = 2; *( up.s++ ) = htole16( (U16) SvUV( src ) ); break; \
			case 'c': *(up.c++) = 1; *( up.c++ ) = (U8) SvIV( src ); break;             \
			case 'C': *(up.c++) = 1; *( up.c++ ) = (U8) SvUV( src ); break;             \
			case 'p': case 'u':                                           \
				up.c = varint( up.c, sv_len(src) );                        \
				memcpy( up.c, SvPV_nolen(src), sv_len(src)  );         \
				up.c += sv_len(src);                                       \
				break;                                                    \
			default:                                       \
				croak("Unsupported format: %s",format);    \
		}                                                  \
	} STMT_END


static inline SV * newSVpvn_pformat ( const char *data, STRLEN size, const unpack_format * format, int idx ) {
	assert(size >= 0);
			if (format && idx < format->size) {
					switch( format->f[ idx ] ) {
						case 'l':
							if (size != 8) warn("Field l should be of size 8, but got: %ju", size);
							return newSViv( le64toh( *( I64 *) data ) );
							break;
						case 'L':
							if (size != 8) warn("Field L should be of size 8, but got: %ju", size);
							return newSVuv( le64toh( *( U64 *) data ) );
							break;
						case 'i':
							if (size != 4) warn("Field i should be of size 4, but got: %ju", size);
							return newSViv( le32toh( *( I32 *) data ) );
							break;
						case 'I':
							if (size != 4) warn("Field I should be of size 4, but got: %ju", size);
							return newSVuv( le32toh( *( U32 *) data ) );
							break;
						case 's':
							if (size != 2) warn("Field s should be of size 2, but got: %ju", size);
							return newSViv( le16toh( *( I16 *) data ) );
							break;
						case 'S':
							if (size != 2) warn("Field S should be of size 2, but got: %ju", size);
							return newSVuv( le16toh( *( U16 *) data ) );
							break;
						case 'c':
							if (size != 1) warn("Field c should be of size 1, but got: %ju", size);
							return newSViv( *( I8 *) data );
							break;
						case 'C':
							if (size != 1) warn("Field C should be of size 1, but got: %ju", size);
							return newSVuv( *( U8 *) data );
							break;
						case 'p':
							return newSVpvn_utf8(data, size, 0);
							break;
						case 'u':
							return newSVpvn_utf8(data, size, 1);
							break;
						default:
							croak("Unsupported format: %s",format->f[ idx ]);
					}
			} else { // no format
				if (format->def == 'u') {
					return newSVpvn_utf8(data, size, 1);
				} else {
					return newSVpvn_utf8(data, size, 0);
				}
			}
}

/*
	should return size of the packet captured.
	return 0 on short read
	return -1 on fatal error
*/

static int parse_reply(HV *ret, const char const *data, STRLEN size, const unpack_format const * format) {
	const char *ptr, *beg, *end;
	
	//warn("parse data of size %d",size);
	if ( size < sizeof(tnt_res_t) ) { // ping could have no count, so + 4
		if ( size >= sizeof(tnt_hdr_t) ) {
			tnt_hdr_t *hx = (tnt_hdr_t *) data;
			//warn ("rcv at least hdr: %d/%d", le32toh( hx->type ), le32toh( hx->len ));
			if ( le32toh( hx->type ) == TNT_OP_PING && le32toh( hx->len ) == 0 ) {
				(void) hv_stores(ret, "code", newSViv( 0 ));
				(void) hv_stores(ret, "status", newSVpvs("ok"));
				(void) hv_stores(ret, "id",   newSViv( le32toh( hx->reqid ) ));
				(void) hv_stores(ret, "type", newSViv( le32toh( hx->type ) ));
				return sizeof(tnt_hdr_t);
			} else {
				//warn("not a ping<%u> or wrong len<%u>!=0 for size=%u", le32toh( hx->type ), le32toh( hx->len ), size);
			}
		}
		//warn("small header");
		goto shortread;
	}
	
	beg = data; // save ptr;
	
	tnt_res_t *hd = (tnt_res_t *) data;
	
	uint32_t type = le32toh( hd->type );
	uint32_t len  = le32toh( hd->len );
	uint32_t code = le32toh( hd->code );
	
	(void) hv_stores(ret, "type", newSViv( type ));
	(void) hv_stores(ret, "code", newSViv( code ));
	(void) hv_stores(ret, "id",   newSViv( le32toh( hd->reqid ) ));
	
	if ( size < len + sizeof(tnt_res_t) - 4 ) {
		//warn("Header ok but wrong len");
		goto shortread;
	}
	
	data += sizeof(tnt_res_t);
	end = data + len - 4;
	
	
	//warn ("type = %d, len=%d (size=%d/%d)", type, len, size, size - sizeof( tnt_hdr_t ));
	switch (type) {
		case TNT_OP_PING:
			return data - beg;
		case TNT_OP_UPDATE:
		case TNT_OP_INSERT:
		case TNT_OP_DELETE:
		case TNT_OP_SELECT:
		case TNT_OP_CALL:
			
			if (code != 0) {
				//warn("error (%d)", end - data - 1);
				(void) hv_stores(ret, "status", newSVpvs("error"));
				(void) hv_stores(ret, "errstr", newSVpvn( data, end > data ? end - data - 1 : 0 ));
				data = end;
				break;
			} else {
				(void) hv_stores(ret, "status", newSVpvs("ok"));
			}
			
			if (data == end) {
				// result without tuples
				//warn("no more data");
				break;
			}
			/*
			if ( len == 0 ) {
				// no tuple data to read.
				//warn("h.len == 0");
				break;
			} else {
				//warn("have more len: %d", len);
			}
			*/
			
			uint32_t count = le32toh( ( *(uint32_t *) data ) );
			//warn ("count = %d",count);
			
			data += 4;
			
			(void) hv_stores(ret, "count", newSViv(count));
			
			if (data == end) {
				// result without tuples
				//warn("no more data");
				break;
			} else {
				//warn("have more data: +%u", end - data);
			}
			
			if (data > end) {
				//warn("data > end");
				data = end;
				break;
			}
			
			int i,k;
			AV *tuples = newAV();
			//warn("count = %d", count);
			if (count < 1024) {
				av_extend(tuples, count);
			}
			
			(void) hv_stores( ret, "tuples", newRV_noinc( (SV *) tuples ) );
			for (i=0;i < count;i++) {
				uint32_t tsize = le32toh( ( *(uint32_t *) data ) ); data += 4;
				//warn("tuple %d size = %u",i,tsize);
				if (data + tsize > end) {
					warn("Intersection1: data=%p, size = %u, end = %p", data, tsize, end);
					goto intersection;
				}
					
				uint32_t cardinality = le32toh( ( *(uint32_t *) data ) ); data +=4;
				
				
				AV *tuple = newAV();
				if (cardinality < 1024) {
					av_extend(tuple, cardinality);
				}
				av_push(tuples, newRV_noinc((SV *)tuple));
				
				//warn("tuple[%d] with cardinality %d", i,cardinality);
				ptr = data;
				data += tsize;
				size -= tsize;
				
				for ( k=0; k < cardinality; k++ ) {
					unsigned int fsize = 0;
					do {
						fsize = ( fsize << 7 ) | ( *ptr & 0x7f );
					} while ( *ptr++ & 0x80 && ptr < end );
					
					if (ptr + fsize > end) {
						warn("Intersection2: k=%d < card=%d (fsize: %d) (ptr: %p :: end: %p)", k, cardinality, fsize, ptr, end);
						goto intersection;
					}
					
					av_push( tuple, newSVpvn_pformat( ptr, fsize, format, k ) );
					ptr += fsize;
				};
			}
			break;
		default:
			(void) hv_stores(ret, "status", newSVpvs("type"));
			(void) hv_stores(ret, "errstr", newSVpvf("Unknown type of operation: 0x%04x", type));
			return end - beg;
	}
	return end - beg;
	
	intersection:
		(void) hv_stores(ret, "status", newSVpvs("intersect"));
		(void) hv_stores(ret, "errstr", newSVpvs("Nested structure intersect packet boundary"));
		return end - beg;
	shortread:
		(void) hv_stores(ret, "status", newSVpvs("buffer"));
		(void) hv_stores(ret, "errstr", newSVpvs("Input data too short"));
		return 0;
}


static inline ptrdiff_t varint_write(char *buf, uint32_t value) {
	char *begin = buf;
	if ( value >= (1 << 7) ) {
		if ( value >= (1 << 14) ) {
			if ( value >= (1 << 21) ) {
				if ( value >= (1 << 28) ) {
					*(buf++) = (value >> 28) | 0x80;
				}
				*(buf++) = (value >> 21) | 0x80;
			}
			*(buf++) = ((value >> 14) | 0x80);
		}
		*(buf++) = ((value >> 7) | 0x80);
	}
	*(buf++) = ((value) & 0x7F);
	return buf - begin;
}

static inline char * varint(char *buf, uint32_t value) {
	if ( value >= (1 << 7) ) {
		if ( value >= (1 << 14) ) {
			if ( value >= (1 << 21) ) {
				if ( value >= (1 << 28) ) {
					*(buf++) = (value >> 28) | 0x80;
				}
				*(buf++) = (value >> 21) | 0x80;
			}
			*(buf++) = ((value >> 14) | 0x80);
		}
		*(buf++) = ((value >> 7) | 0x80);
	}
	*(buf++) = ((value) & 0x7F);
	return buf;
}

int varint_size(uint32_t value) {
	if (value < (1 << 7 )) return 1;
	if (value < (1 << 14)) return 2;
	if (value < (1 << 21)) return 3;
	if (value < (1 << 28)) return 4;
	                       return 5;
}


#define uptr_sv_size( up, svx, need ) \
	STMT_START {                                                           \
		if ( up.c - SvPVX(svx) + need < SvLEN(svx) ) {} \
		else {\
			STRLEN used = up.c - SvPVX(svx); \
			up.c = sv_grow(svx, SvLEN(svx) + need ); \
			up.c += used; \
		}\
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

SV * ping_fast( req_id )
	U32 req_id
	PROTOTYPE: $
	CODE:
		char x[12] = {
			0, 0xff, 0, 0,
			0, 0, 0, 0,
			( req_id & 0xff ),
			( (req_id >> 8) & 0xff ),
			( (req_id >> 16) & 0xff ),
			( (req_id >> 24) & 0xff )
		};
		RETVAL = sv_2mortal( newSVpvn(x,12) );
	OUTPUT:
		RETVAL

void ping( req_id )
	U32 req_id
	
	PROTOTYPE: $
	PPCODE:
		union {
			char      d[12];
			tnt_hdr_t s;
		} buf;
		buf.s.type  = htole32( TNT_OP_PING );
		buf.s.reqid = htole32( req_id );
		buf.s.len   = 0;
		ST(0) = sv_2mortal( newSVpvn(buf.d,12) );
		XSRETURN(1);

void select( req_id, ns, idx, offset, limit, keys, ... )
	U32 req_id
	U32 ns
	U32 idx
	U32 offset
	U32 limit
	AV *keys
	
	PROTOTYPE: $$$$$$;$
	PPCODE:
		register uniptr p;
		int k,i;
		
		dUnpackFormat( format );
		dExtractFormat( format, 6, "select( req_id, ns, idx, offset, limit, keys" );
		
		SV *sv = sv_2mortal(newSVpvn("",0));
		
		tnt_pkt_select_t *h = (tnt_pkt_select_t *)
			SvGROW( sv, 
				( ( (
					sizeof( tnt_pkt_select_t ) +
					+ 4
					+ ( av_len(keys)+1 ) * ( 5 + 32 )
					+ 16
				) >> 5 ) << 5 ) + 0x20
			);
		
		p.c = (char *)(h+1);
		
		for (i = 0; i <= av_len(keys); i++) {
			SV *t = *av_fetch( keys, i, 0 );
			if (!SvROK(t) || (SvTYPE(SvRV(t)) != SVt_PVAV)) croak("keys must be ARRAYREF of ARRAYREF");
			AV *fields = (AV *) SvRV(t);
			
			*( p.i++ ) = htole32( av_len(fields) + 1 );
			
			for (k=0; k <= av_len(fields); k++) {
				SV *f = *av_fetch( fields, k, 0 );
				if ( !SvOK(f) || !sv_len(f) ) {
					*(p.c++) = 0;
				} else {
					uptr_sv_size( p, sv, 5 + sv_len(f) );
					uptr_field_sv_fmt( p, f, k < format.size ? format.f[k] : format.def );
				}
			}
		}
		
		SvCUR_set( sv, p.c - SvPVX(sv) );
		
		h = (tnt_pkt_select_t *) SvPVX( sv ); // for sure
		
		h->type   = htole32( TNT_OP_SELECT );
		h->reqid  = htole32( htole32( req_id ) );
		h->space  = htole32( htole32( ns ) );
		h->index  = htole32( htole32( idx ) );
		h->offset = htole32( htole32( offset ) );
		h->limit  = htole32( htole32( limit ) );
		h->count  = htole32( htole32( av_len(keys) + 1 ) );
		h->len    = htole32( SvCUR(sv) - sizeof( tnt_hdr_t ) );
		
		ST(0) = sv;
		XSRETURN(1);

void insert( req_id, ns, flags, tuple, ... )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV * tuple
	ALIAS:
		insert = TNT_OP_INSERT
		delete = TNT_OP_DELETE
	PROTOTYPE: $$$$;$
	PPCODE:
		register uniptr p;
		int k;
		
		dUnpackFormat( format );
		dExtractFormat( format, 4, "insert( req_id, ns, flags, tuple" );
		
		SV *sv = sv_2mortal(newSVpvn("",0));
		
		tnt_pkt_insert_t *h = (tnt_pkt_insert_t *)
			SvGROW( sv, 
				( ( (
					sizeof( tnt_pkt_insert_t ) +
					+ 4
					+ ( av_len(tuple)+1 ) * ( 5 + 32 )
					+ 16
				) >> 5 ) << 5 ) + 0x20
			);
		
		p.c = (char *)(h+1);
		
		
		*(p.i++) = htole32( av_len(tuple) + 1 );
		
		for (k=0; k <= av_len(tuple); k++) {
			SV *f = *av_fetch( tuple, k, 0 );
			if ( !SvOK(f) || !sv_len(f) ) {
				*(p.c++) = 0;
			} else {
				uptr_sv_size( p, sv, 5 + sv_len(f) );
				uptr_field_sv_fmt( p, f, k < format.size ? format.f[k] : format.def );
			}
		}
		
		SvCUR_set( sv, p.c - SvPVX(sv) );
		h = (tnt_pkt_insert_t *) SvPVX( sv ); // for sure
		h->type   = htole32( ix );
		h->reqid  = htole32( req_id );
		h->space  = htole32( ns );
		h->flags  = htole32( flags );
		h->len    = htole32( SvCUR(sv) - sizeof( tnt_hdr_t ) );
		
		ST(0) = sv;
		XSRETURN(1);

void update( req_id, ns, flags, tuple, ops, ... )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV *tuple
	AV *ops

	PROTOTYPE: $$$$$;$
	PPCODE:
		/*
			packet size:
			hdr: 20 = sizeof( tnt_pkt_update_t )
			tuple: 4
				+ ??? : tuple_size * 16 / variable part
			opcount: 4
				+ ??? : opcount * ( 4 + 1 + 1-5=5 + ?=16 )
				+ ??? : opcount * ( 10 + ?=16 )
			
		*/
		
		register int k;
		register uniptr p;
		
		dUnpackFormat( format );
		dExtractFormat( format, 5, "update( req_id, ns, flags, tuple, operations" );
		
		SV *sv = sv_2mortal(newSVpvn("",0));
		SV **val;
		AV *aop;
		
		tnt_pkt_update_t *h = (tnt_pkt_update_t *)
			SvGROW( sv, 
				( ( (
					sizeof( tnt_pkt_update_t ) +
					+ 4
					+ ( av_len(tuple)+1 ) * ( 5 + 32 )
					+ 4
					+ ( av_len(ops)+1 ) * ( 4 + 1 + 5 + 32 )
					+ 128
				) >> 5 ) << 5 ) + 0x20
			);
		
		p.c = (char *)(h+1);
		
		*( p.i++ ) = htole32( av_len(tuple) + 1 );
		
		for (k=0; k <= av_len(tuple); k++) {
			SV *f = *av_fetch( tuple, k, 0 );
			if ( !SvOK(f) || !sv_len(f) ) {
				p.c += varint_write( p.c, 0 );
			} else {
				uptr_sv_size( p, sv, 5 + sv_len(f) );
				uptr_field_sv_fmt( p, f, k < format.size ? format.f[k] : format.def );
			}
		}
		
		*( p.i++ ) = htole32( av_len(ops) + 1 );
		
		for (k = 0; k <= av_len( ops ); k++) {
			val = av_fetch( ops, k, 0 );
			if (!*val || !SvROK( *val ) || SvTYPE( SvRV(*val) ) != SVt_PVAV )
				croak("Wrong update operation format: %s", val ? SvPV_nolen(*val) : "undef");
			aop = (AV *)SvRV(*val);
			
			if ( av_len( aop ) < 1 ) croak("Too short operation argument list");
			
			*( p.i++ ) = htole32( SvUV( *av_fetch( aop, 0, 0 ) ) );
			
			char *opname = SvPV_nolen( *av_fetch( aop, 1, 0 ) );
			
			U8     opcode = 0;
			
			// Assign and insert allow formats. by default: p
			// Splice always 'p'
			// num ops always force format l or i (32 or 64), depending on size
			
			switch (*opname) {
				case '#': //delete
					*( p.c++ ) = TNT_UPDATE_DELETE;
					*( p.c++ ) = 0;
					break;
				case '=': //set
					//if ( av_len( aop ) < 2 ) croak("Too short operation argument list for %c. Need 3 or 4, have %d", *opname, av_len( aop ) );
					*( p.c++ ) =  TNT_UPDATE_ASSIGN;
					val = av_fetch( aop, 2, 0 );
					if (val && *val && SvOK(*val)) {
						uptr_sv_size( p,sv, 5 + sv_len(*val));
						uptr_field_sv_fmt( p, *val, av_len(aop) > 2 ? *SvPV_nolen( *av_fetch( aop, 3, 0 ) ) : 'p' );
					} else {
						warn("undef in assign");
						*( p.c++ ) = 0;
					}
					break;
				case '!': // insert
					//if ( av_len( aop ) < 2 ) croak("Too short operation argument list for %c. Need 3 or 4, have %d", *opname, av_len( aop ) );
					*( p.c++ ) = TNT_UPDATE_INSERT;
					val = av_fetch( aop, 2, 0 );
					if (val && *val && SvOK(*val)) {
						uptr_sv_size( p,sv, 5 + sv_len(*val));
						uptr_field_sv_fmt( p, *val, av_len(aop) > 2 ? *SvPV_nolen( *av_fetch( aop, 3, 0 ) ) : 'p' );
					} else {
						warn("undef in insert");
						*( p.c++ ) = 0;
					}
					break;
				case ':': //splice
					//if ( av_len( aop ) < 4 ) croak("Too short operation argument list for %c. Need 5, have %d", *opname, av_len( aop ) );
					
					*( p.c++ ) = TNT_UPDATE_SPLICE;
					
					val = av_fetch( aop, 4, 0 );
					
					uptr_sv_size( p,sv, 15 + sv_len(*val));
					
					p.c = varint( p.c, 1+4 + 1+4  + varint_size( sv_len(*val) ) + sv_len(*val) );
					
					*(p.c++) = 4;
					*(p.i++) = (U32)SvIV( *av_fetch( aop, 2, 0 ) );
					*(p.c++) = 4;
					*(p.i++) = (U32)SvIV( *av_fetch( aop, 3, 0 ) );
					
					uptr_field_sv_fmt( p, *val, 'p' );
					break;
				case '+': //add
					opcode = TNT_UPDATE_ADD;
					break;
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
				
				*( p.c++ ) = opcode;
				
				unsigned long long v = SvUV( *av_fetch( aop, 2, 0 ) );
				if (v > 0xffffffff) {
					*( p.c++ ) = 8;
					*( p.q++ ) = (U64) v;
				} else {
					*( p.c++ ) = 4;
					*( p.i++ ) = (U32) v;
				}
			}
		}
		SvCUR_set( sv, p.c - SvPVX(sv) );
		
		h = (tnt_pkt_insert_t *) SvPVX( sv ); // for sure
		
		h->type   = htole32( TNT_OP_UPDATE );
		h->reqid  = htole32( req_id );
		h->space  = htole32( ns );
		h->flags  = htole32( flags );
		h->len    = htole32( SvCUR(sv) - sizeof( tnt_hdr_t ) );
		
		ST(0) = sv;
		XSRETURN(1);


void lua( req_id, flags, proc, tuple, ... )
	unsigned req_id
	unsigned flags
	SV *proc
	AV *tuple

	PROTOTYPE: $$$$;@
	PPCODE:
		register uniptr p;
		dUnpackFormat( format );
		dExtractFormat( format, 4, "lua( req_id, flags, proc, tuple" );
		int k;
		
		SV *sv = sv_2mortal(newSVpvn("",0));
		
		tnt_pkt_call_t *h = (tnt_pkt_call_t *)
			SvGROW( sv, 
				( ( (
					sizeof( tnt_pkt_call_t ) +
					+ 4
					+ sv_len(proc)
					+ ( av_len(tuple)+1 ) * ( 5 + 32 )
					+ 16
				) >> 5 ) << 5 ) + 0x20
			);
		
		p.c = (char *)(h+1);
		
		uptr_field_sv_fmt( p, proc, 'p' );
		
		*(p.i++) = htole32( av_len(tuple) + 1 );
		
		for (k=0; k <= av_len(tuple); k++) {
			SV *f = *av_fetch( tuple, k, 0 );
			if ( !SvOK(f) || !sv_len(f) ) {
				*(p.c++) = 0;
			} else {
				uptr_sv_size( p, sv, 5 + sv_len(f) );
				uptr_field_sv_fmt( p, f, k < format.size ? format.f[k] : format.def );
			}
		}
		
		SvCUR_set( sv, p.c - SvPVX(sv) );
		h = (tnt_pkt_call_t *) SvPVX( sv ); // for sure
		h->type   = htole32( TNT_OP_CALL );
		h->reqid  = htole32( req_id );
		h->flags  = htole32( flags );
		h->len    = htole32( SvCUR(sv) - sizeof( tnt_hdr_t ) );
		
		ST(0) = sv;
		XSRETURN(1);

void peek_size ( sv )
	SV *sv
	
	PROTOTYPE: $
	PPCODE:
		SV * real;
		if ( SvROK(sv) ) {
			switch (SvTYPE( SvRV(sv) )) {
				case SVt_PV:
				case SVt_PVLV:
				case SVt_PVMG:
					real = SvRV( sv );
					break;
				default:
					croak("Bad argument to peek_size: %s, must be scalarref or scalar", SvPV_nolen(sv));
			}
		}
		else
		if ( SvPOK( sv ) ) {
			real = sv;
		}
		else {
			real = 0;
		}
		if( !real || sv_len( real ) < 8 ) {
			ST(0) = sv_2mortal( newSViv(-1) );
		}
		else {
			ST(0) = sv_2mortal( newSVuv( le32toh( *( (U32 *)( SvPV_nolen( real ) + 4 ) ) ) ) );
		}
		XSRETURN(1);

void response( response, ... )
	SV *response

	PROTOTYPE: $;@
	PPCODE:
		if ( !SvOK(response) )
			croak( "response is undefined: %s", SvPV_nolen(response) );
		if ( SvROK(response) ) {
			switch (SvTYPE( SvRV(response) )) {
				case SVt_PVAV:
				case SVt_PVHV:
				case SVt_PVCV:
				case SVt_PVFM:
					croak("Scalar reference required, got %s", SvPV_nolen( response ));
				default:
					response = SvRV(response);
					break;
			}
			/*
			if ( SvOK( SvRV(response) ) ) {
				response = SvRV(response);
			} else {
				croak("Unknown type of reference: Svtype: %d", SvTYPE( SvRV(response) ));
			}
			*/
			/*
			switch (SvTYPE( SvRV(response) )) {
				case SVt_PV:
				case SVt_PVNV: // ???
				case SVt_PVLV:
				case SVt_PVMG:
					response = SvRV(response);
					break;
				default:
					sv_dump( response );
					croak("Unknown type of reference: Svtype: %d", SvTYPE( SvRV(response) ));
			}
			*/
		}
			
		unpack_format format;
		format.f = "";
		format.size = 0;
		format.def = 'p';
		
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
					if (!allowed_format[ (unsigned char) *p++ ])\
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
					format.def = *p;
				} else {
					croak("Bad default_string: %s (%d)", p, l);
				}
			}
		}
		STRLEN size;
		char *data = SvPV( response, size );
		
		HV * hv = newHV();
		int length = parse_reply( hv, data, size, &format );
		if (length > 0) {
			(void) hv_stores(hv, "size", newSVuv(length));
		}
		ST(0) = sv_2mortal(newRV_noinc( (SV *) hv ));
		XSRETURN(1);
