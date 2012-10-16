#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <tarantool/tnt.h>
//#include <tnt.h>
#include <string.h>

#ifdef HAS_QUAD
#ifndef I64
typedef I64TYPE I64;
#endif
#ifndef U64
typedef U64TYPE U64;
#endif
#endif

typedef struct {
	char   *f;
	size_t size;
} unpack_format;

static struct tnt_tuple* tmake_tuple( AV *t ) {
	int i;

	//struct tnt_tuple *r = safemalloc( sizeof( struct tnt_tuple ) );
	struct tnt_tuple *r = tnt_mem_alloc( sizeof( struct tnt_tuple ) );
	if ( !r )
		croak("Can not allocate memory");
	tnt_tuple_init( r );
	// r->alloc = 1;

	for (i = 0; i <= av_len( t ); i++) {
		STRLEN size;
		char *data = SvPV( *av_fetch( t, i, 0 ), size );
		tnt_tuple_add( r, data, size );
	}
	return r;
}

static struct tnt_stream * tmake_buf(void) {
	struct tnt_stream *b = tnt_buf( NULL );
	if ( !b )
		croak("Can not allocate memory");

	return b;
}

static struct tnt_stream *tmake_oplist( AV *ops ) {
	int i;
	struct tnt_stream *b = tmake_buf();

	for (i = 0; i <= av_len( ops ); i++) {
		uint8_t opcode;

		SV *op = *av_fetch( ops, i, 0 );
		if (!SvROK(op) || SvTYPE( SvRV(op) ) != SVt_PVAV)
			croak("Wrong update operation format");
		AV *aop = (AV *)SvRV(op);

		int asize = av_len( aop ) + 1;
		if ( asize < 2 )
			croak("Too short operation argument list");

		unsigned fno = SvIV( *av_fetch( aop, 0, 0 ) );
		STRLEN size;
		char *opname = SvPV( *av_fetch( aop, 1, 0 ), size );


		/* delete */
		if ( strcmp(opname, "delete") == 0 ) {
			tnt_update_delete( b, fno );
			continue;
		}


		if (asize < 3)
			croak("Too short operation argument list");

		/* assign */
		if ( strcmp(opname, "set") == 0 ) {

			char *data = SvPV( *av_fetch( aop, 2, 0 ), size );
			tnt_update_assign( b, fno, data, size );
			continue;
		}

		/* insert */
		if ( strcmp(opname, "insert") == 0 ) {
			char *data = SvPV( *av_fetch( aop, 2, 0 ), size );
			tnt_update_insert( b, fno, data, size );
			continue;
		}


		/* arithmetic operations */
		if ( strcmp(opname, "add") == 0 ) {
			opcode = TNT_UPDATE_ADD;
			goto ARITH;
		}
		if ( strcmp(opname, "and") == 0 ) {
			opcode = TNT_UPDATE_AND;
			goto ARITH;
		}
		if ( strcmp(opname, "or") == 0 ) {
			opcode = TNT_UPDATE_OR;
			goto ARITH;
		}
		if ( strcmp(opname, "xor") == 0 ) {
			opcode = TNT_UPDATE_XOR;
			goto ARITH;
		}


		/* substr */
		if ( strcmp(opname, "substr") == 0 ) {
			if (asize < 4)
				croak("Too short argument list for substr");
			unsigned offset = SvIV( *av_fetch( aop, 2, 0 ) );
			unsigned length = SvIV( *av_fetch( aop, 3, 0 ) );
			char * data;
			if ( asize > 4 && SvOK( *av_fetch( aop, 4, 0 ) ) ) {
			    data = SvPV( *av_fetch( aop, 4, 0 ), size );
			} else {
			    data = "";
			    size = 0;
                        }
			tnt_update_splice( b, fno, offset, length, data, size );
			continue;
		}

		{ /* unknown command */
			char err[512];
			snprintf(err, 512,
				"unknown update operation: `%s'",
				opname
			);
			croak(err);
		}

		ARITH: {
		        unsigned long long v = 0;
			char *data = SvPV( *av_fetch( aop, 2, 0 ), size );
			if (sizeof(v) < size)
			    size = sizeof(v);
			memcpy(&v, data, size); 
			tnt_update_arith( b, fno, opcode, v );
			continue;
		}

	}

	return b;

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
			if (format) {
				if (idx < format->size) {
					switch( format->f[ idx ] ) {
#ifdef HAS_QUAD
						case 'l':
							if (size != 8) warn("Field i should be of sise 8, but got: %d", size);
							av_push( t, newSViv( le64toh( ( I64 ) *data ) ) );
							break;
						case 'L':
							if (size != 8) warn("Field i should be of sise 8, but got: %d", size);
							av_push( t, newSViv( le64toh( ( U64 ) *data ) ) );
							break;
#endif
						case 'i':
							if (size != 4) warn("Field i should be of sise 4, but got: %d", size);
							av_push( t, newSViv( le32toh( ( I32 ) *data ) ) );
							break;
						case 'I':
							if (size != 4) warn("Field i should be of sise 4, but got: %d", size);
							av_push( t, newSViv( le32toh( ( U32 ) *data ) ) );
							break;
						case 's':
							if (size != 2) warn("Field i should be of sise 2, but got: %d", size);
							av_push( t, newSViv( le16toh( ( I32 ) *data ) ) );
							break;
						case 'S':
							if (size != 2) warn("Field i should be of sise 2, but got: %d", size);
							av_push( t, newSViv( le16toh( ( I32 ) *data ) ) );
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
				} else {
					if (default_string == 'u') {
						av_push(t, newSVpvn_utf8(data, size, 1));
					} else {
						av_push(t, newSVpvn_utf8(data, size, 0));
					}
				}
				idx++;
			} else { // no format
				if (default_string == 'u') {
					av_push(t, newSVpvn_utf8(data, size, 1));
				} else {
					av_push(t, newSVpvn_utf8(data, size, 0));
				}
			}
		}
		av_push(res, newRV_noinc((SV *) t));
	}
	return res;
}



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
		warn( "%08x", be32toh( (unsigned int )be ) );

SV * select( req_id, ns, idx, offset, limit, keys )
	unsigned req_id
	unsigned ns
	unsigned idx
	unsigned offset
	unsigned limit
	AV * keys

	PROTOTYPE: $$$$$$
	CODE:
		int i;
		struct tnt_list list;
		tnt_list_init( &list );

		if ( ( list.count = av_len ( keys ) + 1 ) ) {
			list.list = tnt_mem_alloc(
				sizeof( struct tnt_list_ptr ) * list.count
			);


			if ( !list.list )
				return;

			for (i = 0; i < list.count; i++) {
				SV *t = *av_fetch( keys, i, 0 );
				if (!SvROK(t) || (SvTYPE(SvRV(t)) != SVt_PVAV))
					croak("keys must be ARRAYREF"
						" of ARRAYREF"
					);

				list.list[i].ptr = tmake_tuple( (AV *)SvRV(t) );
			}
		}

		struct tnt_stream *s = tmake_buf();
		tnt_stream_reqid( s, req_id );
		tnt_select( s, ns, idx, offset, limit, &list );
		tnt_list_free( &list );


		RETVAL = newSVpvn( TNT_SBUF_DATA(s), TNT_SBUF_SIZE(s) );
		tnt_stream_free( s );

	OUTPUT:
		RETVAL


SV * ping( req_id )
	unsigned req_id

	PROTOTYPE: $
	CODE:
		struct tnt_stream *s = tmake_buf();
		tnt_stream_reqid( s, req_id );
		tnt_ping( s );
		RETVAL = newSVpvn( TNT_SBUF_DATA(s), TNT_SBUF_SIZE(s) );
		tnt_stream_free( s );
		
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

SV * insert( req_id, ns, flags, tuple )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV * tuple

	PROTOTYPE: $$$$
	CODE:
		struct tnt_tuple *t = tmake_tuple( tuple );
		struct tnt_stream *s = tmake_buf();
		tnt_stream_reqid( s, req_id );
		tnt_insert( s, ns, flags, t );
		tnt_tuple_free( t );
		RETVAL = newSVpvn( TNT_SBUF_DATA( s ), TNT_SBUF_SIZE( s ) );
		tnt_stream_free( s );

	OUTPUT:
		RETVAL

SV * update( req_id, ns, flags, tuple, operations )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV *tuple
	AV *operations

	PROTOTYPE: $$$$$
	CODE:
		struct tnt_tuple *t = tmake_tuple( tuple );
		struct tnt_stream *s = tmake_buf();
		struct tnt_stream *ops = tmake_oplist( operations );

		tnt_stream_reqid( s, req_id );
		tnt_update( s, ns, flags, t, ops );
		tnt_tuple_free( t );

		RETVAL = newSVpvn( TNT_SBUF_DATA( s ), TNT_SBUF_SIZE( s ) );

		tnt_stream_free( ops );
		tnt_stream_free( s );


	OUTPUT:
		RETVAL

SV * delete( req_id, ns, flags, tuple )
	unsigned req_id
	unsigned ns
	unsigned flags
	AV *tuple

	PROTOTYPE: $$$$
	CODE:
		struct tnt_tuple *t = tmake_tuple( tuple );
		struct tnt_stream *s = tmake_buf();
		tnt_stream_reqid( s, req_id );
		tnt_delete( s, ns, flags, t );
		tnt_tuple_free( t );
		RETVAL = newSVpvn( TNT_SBUF_DATA( s ), TNT_SBUF_SIZE( s ) );
		tnt_stream_free( s );
	OUTPUT:
		RETVAL

SV * lua( req_id, flags, proc, tuple )
	unsigned req_id
	unsigned flags
	char *proc
	AV *tuple

	PROTOTYPE: $$$$
	CODE:
		struct tnt_tuple *t = tmake_tuple( tuple );
		struct tnt_stream *s = tmake_buf();
		tnt_stream_reqid( s, req_id );
		tnt_call( s, flags, proc, t );
		tnt_tuple_free( t );
		RETVAL = newSVpvn( TNT_SBUF_DATA( s ), TNT_SBUF_SIZE( s ) );
		tnt_stream_free( s );
	OUTPUT:
		RETVAL



HV * response( response, ... )
	SV *response

	PROTOTYPE: $;$$
	INIT:
		RETVAL = newHV();
		sv_2mortal((SV *)RETVAL);
	CODE:
		if ( !SvOK(response) )
			croak( "response is undefined" );
			
		unpack_format format;
		char default_string = 'p';
		
		if (items > 1) {
			if ( SvPOK(ST(1)) ) {
				char *p = format.f = SvPVbyte(ST(1), format.size);
				while(*p) {
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
				}
				warn("Used format [%lu]: %s", format.size, format.f);
			} else {
				croak("Usege: response(packet [ , format_string [, default_string=[p|u] ] ])");
			}
			if ( items > 2 && SvPOK(ST(2)) ) {
				STRLEN l;
				char * p = SvPV( ST(2), l );
				if (l == 1 && ( *p == 'p' || *p == 'u' )) {
					default_string=*p;
				} else {
					croak("X: %s (%d)", p, l);
				}
			}
		}
		STRLEN size;
		char *data = SvPV( response, size );
		struct tnt_reply reply;
		
		
		tnt_reply_init( &reply );
		size_t offset = 0;
		int cnt = tnt_reply( &reply, data, size, &offset );
		int i, j;
		
		if ( cnt != 0 ) {
			if (cnt < 0) {
				hv_stores(RETVAL, "status", newSVpvs("fatal"));
				hv_stores(RETVAL, "errstr", newSVpvs("Can't parse server response"));
			} else {
				hv_stores(RETVAL, "status", newSVpvs("buffer"));
				hv_stores(RETVAL, "errstr", newSVpvs("Input data too short"));
			}
			if (reply.code)
				hv_stores(RETVAL, "code",   newSViv(reply.code));
			if (reply.reqid)
				hv_stores(RETVAL, "id",   newSViv(reply.reqid));
			if (reply.op)
				hv_stores(RETVAL, "op",   newSViv(reply.op));
		} else
		{
			hv_stores(RETVAL, "code",    newSViv(reply.code));
			hv_stores(RETVAL, "id",      newSViv(reply.reqid));
			hv_stores(RETVAL, "op",      newSViv(reply.op));
			hv_stores(RETVAL, "count",   newSViv(reply.count));
			if (reply.code) {
				hv_stores(RETVAL, "status", newSVpvs("error"));
				hv_stores(RETVAL, "errstr", newSVpv(reply.error,0));
			} else {
				hv_stores(RETVAL, "status", newSVpvs("ok"));
				AV *tuples = extract_tuples( &reply, &format, default_string );
/*
				if (format) {
					int idx,i,minlen;
					AV *tuple;
					SV **val, **fmt;
					for (idx = 0; idx <= av_len(tuples); idx++) {
						val = av_fetch(tuples, idx, 0);
						if (val && *val && SvROK(*val) && SvTYPE(SvRV(*val)) == SVt_PVAV ) {
							// ...
						}
						else { croak ("Bad tuple data: %s", SvPV_nolen(*val)); }
						minlen = av_len( format ) > av_len( *val ) ? av_len( *val ) : av_len( format );
						for (i = 0; i <= minlen; i++) {
						}
					}
				}
*/
/*
				I32 
				SV **tv, **fv;
				if (format) {
					int i;
					int minlen = av_len( format );
					if (minlen > av_len( tuples ) )
						minlen = av_len( tuples );
					for (i=0; i < minlen; i++) {
						if ( tv = av_fetch( tuples ) )
						break;
					}
				}
*/
				
				hv_stores(RETVAL, "tuples", newRV((SV *)tuples));
			}
		}
		tnt_reply_free( &reply );
	
	OUTPUT:
		RETVAL

