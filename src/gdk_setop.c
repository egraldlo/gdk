/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#line 23 "gdk_setop.mx"
/*
 * @a Peter Boncz
 *
 * @* Set Operations
 * Set operations are provided in two series:
 * @itemize
 * @item
 * k-@emph{operand}, which look only at the head column.
 * @item
 * s-@emph{operand} series, that look at the whole BUN.
 * @end itemize
 *
 * Operands provided are:
 * @itemize
 * @item [s,k]unique
 * produces a copy of the bat, with double elimination
 * @item [s,k]union
 * produces a bat union.
 * @item [s,k]diff
 * produces bat difference.
 * @item [s,k]intersection
 * produce bat intersection.
 * @end itemize
 *
 * Implementations typically take two forms: if the input relation(s)
 * is/are ordered, a merge-algorithm is used. Otherwise, hash-indices
 * are produced on demand for the hash-based versions.
 *
 * The @emph{[k,s]intersect(l,r)} operations result in all BUNs of
 * @emph{l} that are also in @emph{r}. They do not do
 * double-elimination over the @emph{l} BUNs.
 *
 * The @emph{[k,s]diff(l,r)} operations result in all BUNs of @emph{l}
 * that are not in @emph{r}. They do not do double-elimination over
 * the @emph{l} BUNs.
 *
 * The @emph{[k,s]union(l,r)} operations result in all BUNs of
 * @emph{l}, plus all BUNs of @emph{r} that are not in @emph{l}. They
 * do not do double-elimination over the @emph{l} nor @emph{r} BUNs.
 *
 * Operations with double-elimination can be formed by performing
 * @emph{[k,s]unique(l)} on their operands.
 *
 * The @emph{kintersect(l,r)} is used also as implementation for the
 * @emph{semijoin()}.
 */
/*
 * 数据库的集合操作
 * */

#line 70 "gdk_setop.mx"
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_search.h"

#define HITk(t1,t2)		TRUE
#define HITs(t1,t2)		((*cmp)(t1,t2) == 0)
#define EQUALs(t1,t2)		((*cmp)(t1,t2) == 0 && (*cmp)(t1,tnil))
#define EQUALk(t1,t2)		TRUE
#define FLIPs			TRUE
#define FLIPk			FALSE

#define HITintersect(h,t)       bunfastins(bn,h,t)
#define HITdiff(h,t)
#define MISSintersect(h,t)
#define MISSdiff(h,t)           bunfastins(bn,h,t)

#define HITintersect_nocheck(h,t)       bunfastins_nocheck(bn,BUNlast(bn),h,t,Hsize(bn),Tsize(bn))
#define HITdiff_nocheck(h,t)
#define MISSintersect_nocheck(h,t)
#define MISSdiff_nocheck(h,t)           bunfastins_nocheck(bn,BUNlast(bn),h,t,Hsize(bn),Tsize(bn))

#define DHITintersect(h,t)       bnh[o] = *(h); bnt[o++] = t;
#define DHITdiff(h,t)
#define DMISSintersect(h,t)
#define DMISSdiff(h,t)           bnh[o] = *(h); bnt[o++] = t;

#define ENDintersect(h,t)
#define ENDdiff(h,t)            for(;p1<q1;p1++) bunfastins(bn,h,t)

/*
 * @+ Double Elimination
 * Comes in two flavors: looking at one column, or at two at-a-time.
 * Implementation is either merge- or hash-based.
 */


#line 204 "gdk_setop.mx"
static BAT *
BATins_kunique(BAT *bn, BAT *b)
{
	bit unique = FALSE;
	BATiter bi = bat_iterator(b);
	BATiter bni = bat_iterator(bn);

	BATcheck(b, "BATins_kunique: src BAT required");
	BATcheck(bn, "BATins_kunique: dst BAT required");
	unique = (BATcount(bn) == 0);
	
#line 173 "gdk_setop.mx"
	switch (ATOMstorage(b->htype)) {
	case TYPE_bte:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),bte) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextlocvar_bte;
		}
		bunfastins(bn, h, t);
  nextlocvar_bte:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),bte) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextlocloc_bte;
		}
		bunfastins(bn, h, t);
  nextlocloc_bte:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_bte(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_bte(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_bte(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_bte(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 175 "gdk_setop.mx"

	case TYPE_sht:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),sht) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextlocvar_sht;
		}
		bunfastins(bn, h, t);
  nextlocvar_sht:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),sht) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextlocloc_sht;
		}
		bunfastins(bn, h, t);
  nextlocloc_sht:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_sht(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_sht(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_sht(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_sht(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 177 "gdk_setop.mx"

	case TYPE_int:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),int) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextlocvar_int;
		}
		bunfastins(bn, h, t);
  nextlocvar_int:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),int) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextlocloc_int;
		}
		bunfastins(bn, h, t);
  nextlocloc_int:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_int(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_int(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_int(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_int(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 179 "gdk_setop.mx"

	case TYPE_flt:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),flt) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextlocvar_flt;
		}
		bunfastins(bn, h, t);
  nextlocvar_flt:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),flt) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextlocloc_flt;
		}
		bunfastins(bn, h, t);
  nextlocloc_flt:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_flt(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_flt(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_flt(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_flt(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 181 "gdk_setop.mx"

	case TYPE_dbl:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),dbl) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextlocvar_dbl;
		}
		bunfastins(bn, h, t);
  nextlocvar_dbl:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),dbl) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextlocloc_dbl;
		}
		bunfastins(bn, h, t);
  nextlocloc_dbl:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_dbl(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_dbl(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_dbl(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_dbl(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 183 "gdk_setop.mx"

	case TYPE_lng:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),lng) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextlocvar_lng;
		}
		bunfastins(bn, h, t);
  nextlocvar_lng:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),lng) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextlocloc_lng;
		}
		bunfastins(bn, h, t);
  nextlocloc_lng:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_lng(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_lng(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_lng(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_lng(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 185 "gdk_setop.mx"

	case TYPE_str:
		if (b->H->vheap->hashash) {
			
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && GDK_STRCMP(h,BUNhvar(bi,r)) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextvarvar_str_hv;
		}
		bunfastins(bn, h, t);
  nextvarvar_str_hv:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && GDK_STRCMP(h,BUNhvar(bi,r)) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextvarloc_str_hv;
		}
		bunfastins(bn, h, t);
  nextvarloc_str_hv:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_str_hv(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_str_hv(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_str_hv(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_str_hv(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 188 "gdk_setop.mx"

		}
		/* fall through */
	default:
		{
			int (*merge)(const void *, const void *) = BATatoms[b->htype].atomCmp;

			if (b->hvarsized) {
				
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhvar(bi,r))) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextvarvarvar;
		}
		bunfastins(bn, h, t);
  nextvarvarvar:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhvar(bi,r))) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextvarlocvar;
		}
		bunfastins(bn, h, t);
  nextvarlocvar:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloopvar(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsvar(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloopvar(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsvar(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 196 "gdk_setop.mx"

			} else {
				
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_kunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhloc(bi,r))) == 0; r++) {
			if (HITk(t, BUNtvar(bi, r)))
				goto nextlocvarloc;
		}
		bunfastins(bn, h, t);
  nextlocvarloc:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhloc(bi,r))) == 0; r++) {
			if (HITk(t, BUNtloc(bi, r)))
				goto nextloclocloc;
		}
		bunfastins(bn, h, t);
  nextloclocloc:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHlooploc(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsloc(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_kunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHlooploc(bni, bn->H->hash, yy, h) {
			if (HITk(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsloc(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 198 "gdk_setop.mx"

			}
		}
	}


#line 214 "gdk_setop.mx"

	if (unique && bn->hkey == FALSE) {
		/* we inserted unique head-values into an empty BAT;
		   hence, the resulting BAT's head is (now) unique/key ... */
		BATkey(bn, TRUE);
	}
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

static BAT *
BATins_sunique(BAT *bn, BAT *b)
{
	bit unique = FALSE;
	BUN fst1, fst2, last1, last2;
	BATiter bi = bat_iterator(b);
	BATiter bni = bat_iterator(bn);

	BATcheck(b, "BATins_sunique: src BAT required");
	BATcheck(bn, "BATins_sunique: dst BAT required");

	unique = (BATcount(bn) == 0);

	fst1 = BUNfirst(bn);
	fst2 = BUNfirst(b);

	last1 = (BUNlast(bn) - 1);
	last2 = (BUNlast(b) - 1);

	if (BATcount(b) &&
	    BAThordered(b) &&
	    ATOMcmp(b->htype, BUNhead(bi, fst2), BUNhead(bi, last2)) == 0 &&
	    (BATcount(bn) == 0 ||
	     (ATOMcmp(bn->htype, BUNhead(bni, fst1), BUNhead(bi, fst2)) == 0 &&
	      BAThordered(bn) &&
	      ATOMcmp(bn->htype, BUNhead(bni, fst1), BUNhead(bni, last1)) == 0))) {
		ALGODEBUG fprintf(stderr, "#BATins_sunique: BATins_kunique(BATmirror(bn), BATmirror(b))\n");
		return BATins_kunique(BATmirror(bn), BATmirror(b));
	}
	if (BATcount(b) &&
	    BATtordered(b) &&
	    ATOMcmp(b->ttype, BUNtail(bi, fst2), BUNtail(bi, last2)) == 0 &&
	    (BATcount(bn) == 0 ||
	     (ATOMcmp(bn->ttype, BUNtail(bni, fst1), BUNtail(bi, fst2)) == 0 &&
	      BATtordered(bn) &&
	      ATOMcmp(bn->ttype, BUNtail(bni, fst1), BUNtail(bni, last1)) == 0))) {
		ALGODEBUG fprintf(stderr, "#BATins_sunique: BATins_kunique(bn, b)\n");
		return BATins_kunique(bn, b);
	}
	if (BATtordered(b) && ATOMstorage(b->ttype) < TYPE_str) {
		bni.b = bn = BATmirror(bn);
		bi.b = b = BATmirror(b);
	}

	
#line 173 "gdk_setop.mx"
	switch (ATOMstorage(b->htype)) {
	case TYPE_bte:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),bte) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextlocvar_bte;
		}
		bunfastins(bn, h, t);
  nextlocvar_bte:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),bte) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextlocloc_bte;
		}
		bunfastins(bn, h, t);
  nextlocloc_bte:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_bte(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_bte(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_bte(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_bte(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 175 "gdk_setop.mx"

	case TYPE_sht:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),sht) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextlocvar_sht;
		}
		bunfastins(bn, h, t);
  nextlocvar_sht:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),sht) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextlocloc_sht;
		}
		bunfastins(bn, h, t);
  nextlocloc_sht:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_sht(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_sht(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_sht(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_sht(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 177 "gdk_setop.mx"

	case TYPE_int:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),int) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextlocvar_int;
		}
		bunfastins(bn, h, t);
  nextlocvar_int:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),int) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextlocloc_int;
		}
		bunfastins(bn, h, t);
  nextlocloc_int:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_int(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_int(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_int(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_int(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 179 "gdk_setop.mx"

	case TYPE_flt:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),flt) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextlocvar_flt;
		}
		bunfastins(bn, h, t);
  nextlocvar_flt:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),flt) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextlocloc_flt;
		}
		bunfastins(bn, h, t);
  nextlocloc_flt:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_flt(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_flt(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_flt(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_flt(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 181 "gdk_setop.mx"

	case TYPE_dbl:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),dbl) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextlocvar_dbl;
		}
		bunfastins(bn, h, t);
  nextlocvar_dbl:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),dbl) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextlocloc_dbl;
		}
		bunfastins(bn, h, t);
  nextlocloc_dbl:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_dbl(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_dbl(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_dbl(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_dbl(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 183 "gdk_setop.mx"

	case TYPE_lng:
		
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),lng) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextlocvar_lng;
		}
		bunfastins(bn, h, t);
  nextlocvar_lng:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && simple_CMP(h,BUNhloc(bi,r),lng) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextlocloc_lng;
		}
		bunfastins(bn, h, t);
  nextlocloc_lng:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_lng(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_lng(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_lng(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_lng(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 185 "gdk_setop.mx"

	case TYPE_str:
		if (b->H->vheap->hashash) {
			
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && GDK_STRCMP(h,BUNhvar(bi,r)) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextvarvar_str_hv;
		}
		bunfastins(bn, h, t);
  nextvarvar_str_hv:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && GDK_STRCMP(h,BUNhvar(bi,r)) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextvarloc_str_hv;
		}
		bunfastins(bn, h, t);
  nextvarloc_str_hv:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_str_hv(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_str_hv(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloop_str_hv(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHins_str_hv(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 188 "gdk_setop.mx"

		}
		/* fall through */
	default:
		{
			int (*merge)(const void *, const void *) = BATatoms[b->htype].atomCmp;

			if (b->hvarsized) {
				
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhvar(bi,r))) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextvarvarvar;
		}
		bunfastins(bn, h, t);
  nextvarvarvar:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhvar(bi,r))) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextvarlocvar;
		}
		bunfastins(bn, h, t);
  nextvarlocvar:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloopvar(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsvar(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhvar(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHloopvar(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsvar(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 196 "gdk_setop.mx"

			} else {
				
#line 149 "gdk_setop.mx"
	{
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp;
		BUN zz;
		BUN p, q, r;

		if (BAThordered(b)) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: BAThordered(b)\n");
			ALGODEBUG fprintf(stderr, "#BATins_sunique: mergeelim\n");
			if (b->tvarsized) {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtvar(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhloc(bi,r))) == 0; r++) {
			if (HITs(t, BUNtvar(bi, r)))
				goto nextlocvarloc;
		}
		bunfastins(bn, h, t);
  nextlocvarloc:;
	}


#line 158 "gdk_setop.mx"

			} else {
				
#line 106 "gdk_setop.mx"
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi,p);
		ptr t = BUNtloc(bi,p);

		for (r = p + 1; r < q && ((*merge)(h,BUNhloc(bi,r))) == 0; r++) {
			if (HITs(t, BUNtloc(bi, r)))
				goto nextloclocloc;
		}
		bunfastins(bn, h, t);
  nextloclocloc:;
	}


#line 160 "gdk_setop.mx"

			}
		} else if (b->tvarsized) {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtvar(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHlooploc(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtvar(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsloc(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 164 "gdk_setop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATins_sunique: hashelim\n");
			
#line 118 "gdk_setop.mx"
	zz = BUNfirst(bn);
	if (!bn->H->hash) {
		if (BAThash(bn, BATcapacity(bn)) == NULL) {
			BBPreclaim(bn);
			return NULL;
		}
	}
	BATloop(b, p, q) {
		ptr h = BUNhloc(bi, p);
		ptr t = BUNtloc(bi, p);
		int ins = 1;
		BUN yy;

		if (BATprepareHash(bn)) {
			BBPreclaim(bn);
			return NULL;
		}
		HASHlooploc(bni, bn->H->hash, yy, h) {
			if (HITs(t, BUNtloc(bni, yy))) {
				ins = 0;
				break;
			}
		}
		if (ins) {
			bunfastins(bn, h, t);
			if (bn->H->hash)
				HASHinsloc(bn->H->hash, zz, h);
			zz++;
		}
	}


#line 167 "gdk_setop.mx"

		}
		(void) cmp;
		break;
	}


#line 198 "gdk_setop.mx"

			}
		}
	}


#line 272 "gdk_setop.mx"

	if (unique && bn->batSet == FALSE) {
		/* we inserted unique BUNs into an empty BAT;
		   hence, the resulting BAT is (now) unique/set ... */
		BATset(bn, TRUE);
	}
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


/*
 * @- Unique
 * The routine BATsunique removes duplicate BUNs,
 * The routine BATkunique removes duplicate head entries.
 */
BAT *
BATkunique(BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATkunique");

	if (b->hkey) {
		bn = BATcopy(b, b->htype, b->ttype, FALSE);
		if (bn == NULL)
			return NULL;
	} else {
		BUN cnt = BATcount(b);

		if (cnt > 10000) {
			BAT *tmp2 = NULL, *tmp1, *tmp0 = VIEWhead_(b, BAT_WRITE);

			if (tmp0) {
				tmp1 = BATsample(tmp0, 1000);
				if (tmp1) {
					tmp2 = BATkunique(tmp1);
					if (tmp2) {
						cnt = (BUN) ((((lng) BATcount(tmp2)) * cnt) / 900);
						BBPreclaim(tmp2);
					}
					BBPreclaim(tmp1);
				}
				BBPreclaim(tmp0);
			}
			if (tmp2 == NULL)
				return NULL;
		}
		bn = BATnew(BAThtype(b), BATttype(b), cnt);
		if (bn == NULL || BATins_kunique(bn, b) == NULL)
			return NULL;
	}

	/* property management */
	if (b->halign == 0) {
		b->halign = OIDnew(1);
	}
	BATkey(BATmirror(bn), BATtkey(b));
	bn->hsorted = BAThordered(b);
	bn->hrevsorted = BAThrevordered(b);
	bn->tsorted = BATtordered(b);
	bn->trevsorted = BATtrevordered(b);
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	if (BATcount(bn) == BATcount(b)) {
		ALIGNset(bn, b);
	}
	BATkey(bn, TRUE);	/* this we accomplished */
	return bn;
}

BAT *
BATsunique(BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATsunique");

	if (b->hkey || b->tkey || b->batSet) {
		bn = BATcopy(b, b->htype, b->ttype, FALSE);
	} else {
		BUN cnt = BATcount(b);

		if (cnt > 10000) {
			BAT *tmp2 = NULL, *tmp1 = BATsample(b, 1000);

			if (tmp1) {
				tmp2 = BATkunique(tmp1);
				if (tmp2) {
					cnt = BATcount(tmp2) * (cnt / 1000);
					BBPreclaim(tmp2);
				}
				BBPreclaim(tmp1);
			}
			if (tmp2 == NULL)
				return NULL;
		}
		bn = BATnew(BAThtype(b), BATttype(b), cnt);
		if (bn == NULL || BATins_sunique(bn, b) == NULL)
			return NULL;
	}

	/* property management */
	BATkey(bn, BAThkey(b));
	BATkey(BATmirror(bn), BATtkey(b));
	bn->hsorted = BAThordered(b);
	bn->hrevsorted = BAThrevordered(b);
	bn->tsorted = BATtordered(b);
	bn->trevsorted = BATtrevordered(b);
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	if (BATcount(bn) == BATcount(b)) {
		ALIGNset(bn, b);
	}
	BATset(bn, TRUE);	/* this we accomplished */
	return bn;
}

/*
 * @+ Difference and Intersect
 * Difference and Intersection are handled together. For each routine
 * there are two versions: BATkdiff(l,r) and
 * BATkintersect(l,r) (which look at the head column only), versus
 * BATsdiff(l,r) and BATsintersect(l,r) (looking at both
 * columns).  TODO synced/key case..
 */


#line 482 "gdk_setop.mx"
#define DIRECT_MAX 256

#define bte_EQ(x,y) simple_EQ(x,y,bte)
#define sht_EQ(x,y) simple_EQ(x,y,sht)
#define int_EQ(x,y) simple_EQ(x,y,int)
#define lng_EQ(x,y) simple_EQ(x,y,lng)
#define flt_EQ(x,y) simple_EQ(x,y,flt)
#define dbl_EQ(x,y) simple_EQ(x,y,dbl)

/* later add version for l void tail, remove general tail values then */


#line 753 "gdk_setop.mx"


#line 615 "gdk_setop.mx"
static BAT*
BATins_sintersect(BAT *bn, BAT *l, BAT *r)
{
	int hash = TRUE, (*cmp)(const void *, const void *), (*merge)(const void *, const void *) = NULL;
	ptr hnil, tnil;
	BAT *b = bn;

	/* determine how to do the intersect */
	if (BAThordered(l) & BAThordered(r)) {
		hash = FALSE;
	}
#if FLIPs
	else {
		int flip = BATtordered(l) & BATtordered(r);

		if (flip) {
			hash = FALSE;
		} else {
			flip = r->H->hash == NULL && r->T->hash != NULL;
		}
		if (flip) {
			r = BATmirror(r);
			l = BATmirror(l);
			bn = BATmirror(bn);
		}
	}
#endif
	merge = BATatoms[l->htype].atomCmp;
	cmp = BATatoms[l->ttype].atomCmp;
	hnil = ATOMnilptr(l->htype);
	tnil = ATOMnilptr(l->ttype);
	(void) cmp;
	(void) tnil;
	(void) hnil;

	if (BAThdense(r)) {
		/* voidcheck */
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BUN p1 = BUNfirst(r), q1 = BUNlast(r);
		oid rl = * (oid *) BUNhead(ri, p1);
		oid rh = rl + BATcount(r);
		ptr h, t = NULL, t2 = NULL;

		(void) t2;

		ALGODEBUG fprintf(stderr, "#BATins_sintersect: voidcheck[s, intersect];\n");
		if (BAThdense(l)) {
			oid ll = * (oid *) BUNhead(li, (p1 = BUNfirst(l)));
			oid lh = ll + BATcount(l);
			BUN hit_start = (q1 = BUNlast(l)), hit_end = q1, w = BUNfirst(r);
			BUN off = p1;

			h = (ptr) &ll;

			if (rl >= ll && rl < lh) {
				hit_start = off + (rl - ll);
			} else if (rl < ll && rh > ll) {
				hit_start = p1;
				w += (ll - rl);
			}
			if (rh >= ll && rh < lh) {
				hit_end = off + (rh - ll);
			}
			while(p1 < hit_start) {
				t = BUNtail(li, p1);
				MISSintersect(h, t);
				ll++;
				p1++;
			}
			while(p1 < hit_end) {
				t = BUNtail(li, p1);
				t2 = BUNtail(ri, w);
				if (EQUALs(t, t2)) {
					HITintersect(h, t);
				} else {
					MISSintersect(h, t);
				}
				ll++;
				p1++;
				w++;
			}
			while (p1 < q1) {
				t = BUNtail(li, p1);
				MISSintersect(h, t);
				ll++;
				p1++;
			}
		} else {
			BUN off = p1;

			BATloop(l, p1, q1) {
				oid o = * (oid *) BUNhloc(li, p1);

				h = (ptr) &o;
				t = BUNtail(li, p1);

				if (o >= rl && o < rh) {
					BUN w = off + (o - rl);

					t2 = BUNtail(ri, w);
					if (EQUALs(t, t2)) {
						HITintersect(h, t);
						continue;
					}
				}
				MISSintersect(h, t);
			}
		}
	} else {
		switch(ATOMstorage(r->htype)) {
		case TYPE_bte:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _bte, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, bte, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_bte(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,bte))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _bte, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			bte *h = (bte*)BUNhloc(li, BUNfirst(l));
			bte *rh = (bte*)BUNhloc(ri, 0);
			bte *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _bte, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (bte*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			bte *h = (bte*)BUNhloc(li, 0);
			bte *rh = (bte*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _bte, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _bte, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _bte, simple_CMP(h,h2,bte), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,bte)) > 0) {
				if ((++p2) >= q2)
					goto endloc_bte;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,bte)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,bte)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_bte:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 727 "gdk_setop.mx"

		case TYPE_sht:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _sht, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, sht, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_sht(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,sht))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _sht, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			sht *h = (sht*)BUNhloc(li, BUNfirst(l));
			sht *rh = (sht*)BUNhloc(ri, 0);
			sht *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _sht, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (sht*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			sht *h = (sht*)BUNhloc(li, 0);
			sht *rh = (sht*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _sht, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _sht, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _sht, simple_CMP(h,h2,sht), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,sht)) > 0) {
				if ((++p2) >= q2)
					goto endloc_sht;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,sht)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,sht)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_sht:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 729 "gdk_setop.mx"

		case TYPE_int:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _int, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, int, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_int(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,int))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _int, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			int *h = (int*)BUNhloc(li, BUNfirst(l));
			int *rh = (int*)BUNhloc(ri, 0);
			int *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _int, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (int*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			int *h = (int*)BUNhloc(li, 0);
			int *rh = (int*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _int, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _int, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _int, simple_CMP(h,h2,int), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,int)) > 0) {
				if ((++p2) >= q2)
					goto endloc_int;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,int)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,int)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_int:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 731 "gdk_setop.mx"

		case TYPE_flt:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _flt, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, flt, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_flt(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,flt))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _flt, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			flt *h = (flt*)BUNhloc(li, BUNfirst(l));
			flt *rh = (flt*)BUNhloc(ri, 0);
			flt *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _flt, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (flt*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			flt *h = (flt*)BUNhloc(li, 0);
			flt *rh = (flt*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _flt, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _flt, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _flt, simple_CMP(h,h2,flt), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,flt)) > 0) {
				if ((++p2) >= q2)
					goto endloc_flt;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,flt)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,flt)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_flt:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 733 "gdk_setop.mx"

		case TYPE_dbl:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _dbl, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, dbl, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_dbl(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,dbl))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _dbl, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			dbl *h = (dbl*)BUNhloc(li, BUNfirst(l));
			dbl *rh = (dbl*)BUNhloc(ri, 0);
			dbl *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _dbl, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (dbl*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			dbl *h = (dbl*)BUNhloc(li, 0);
			dbl *rh = (dbl*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _dbl, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _dbl, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _dbl, simple_CMP(h,h2,dbl), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,dbl)) > 0) {
				if ((++p2) >= q2)
					goto endloc_dbl;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,dbl)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,dbl)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_dbl:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 735 "gdk_setop.mx"

		case TYPE_lng:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _lng, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, lng, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_lng(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,lng))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _lng, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			lng *h = (lng*)BUNhloc(li, BUNfirst(l));
			lng *rh = (lng*)BUNhloc(ri, 0);
			lng *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _lng, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (lng*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			lng *h = (lng*)BUNhloc(li, 0);
			lng *rh = (lng*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _lng, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _lng, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _lng, simple_CMP(h,h2,lng), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,lng)) > 0) {
				if ((++p2) >= q2)
					goto endloc_lng;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,lng)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,lng)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_lng:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 737 "gdk_setop.mx"

		default:
			if (r->hvarsized) {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, var, var, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectvar: hashcheck[intersect, var, var, _str_hv, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectvar: hashcheck[intersect, var, var, var, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectvar: mergecheck[intersect, var, var, ((*merge)(h,h2)), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhvar(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhvar(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endvarvar;
				h2 = BUNhvar(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhvar(ri, pb);
								if (((*merge)(h,h2))) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endvarvar:;
	ENDintersect(BUNhvar(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 740 "gdk_setop.mx"

			} else {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, loc, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _str_hv, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, loc, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, loc, ((*merge)(h,h2)), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endlocloc;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (((*merge)(h,h2))) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endlocloc:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 742 "gdk_setop.mx"

			}
		}
	}
	return b;
  bunins_failed:
	BBPreclaim(b);
	return NULL;
}


#line 754 "gdk_setop.mx"


#line 615 "gdk_setop.mx"
static BAT*
BATins_sdiff(BAT *bn, BAT *l, BAT *r)
{
	int hash = TRUE, (*cmp)(const void *, const void *), (*merge)(const void *, const void *) = NULL;
	ptr hnil, tnil;
	BAT *b = bn;

	/* determine how to do the intersect */
	if (BAThordered(l) & BAThordered(r)) {
		hash = FALSE;
	}
#if FLIPs
	else {
		int flip = BATtordered(l) & BATtordered(r);

		if (flip) {
			hash = FALSE;
		} else {
			flip = r->H->hash == NULL && r->T->hash != NULL;
		}
		if (flip) {
			r = BATmirror(r);
			l = BATmirror(l);
			bn = BATmirror(bn);
		}
	}
#endif
	merge = BATatoms[l->htype].atomCmp;
	cmp = BATatoms[l->ttype].atomCmp;
	hnil = ATOMnilptr(l->htype);
	tnil = ATOMnilptr(l->ttype);
	(void) cmp;
	(void) tnil;
	(void) hnil;

	if (BAThdense(r)) {
		/* voidcheck */
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BUN p1 = BUNfirst(r), q1 = BUNlast(r);
		oid rl = * (oid *) BUNhead(ri, p1);
		oid rh = rl + BATcount(r);
		ptr h, t = NULL, t2 = NULL;

		(void) t2;

		ALGODEBUG fprintf(stderr, "#BATins_sdiff: voidcheck[s, diff];\n");
		if (BAThdense(l)) {
			oid ll = * (oid *) BUNhead(li, (p1 = BUNfirst(l)));
			oid lh = ll + BATcount(l);
			BUN hit_start = (q1 = BUNlast(l)), hit_end = q1, w = BUNfirst(r);
			BUN off = p1;

			h = (ptr) &ll;

			if (rl >= ll && rl < lh) {
				hit_start = off + (rl - ll);
			} else if (rl < ll && rh > ll) {
				hit_start = p1;
				w += (ll - rl);
			}
			if (rh >= ll && rh < lh) {
				hit_end = off + (rh - ll);
			}
			while(p1 < hit_start) {
				t = BUNtail(li, p1);
				MISSdiff(h, t);
				ll++;
				p1++;
			}
			while(p1 < hit_end) {
				t = BUNtail(li, p1);
				t2 = BUNtail(ri, w);
				if (EQUALs(t, t2)) {
					HITdiff(h, t);
				} else {
					MISSdiff(h, t);
				}
				ll++;
				p1++;
				w++;
			}
			while (p1 < q1) {
				t = BUNtail(li, p1);
				MISSdiff(h, t);
				ll++;
				p1++;
			}
		} else {
			BUN off = p1;

			BATloop(l, p1, q1) {
				oid o = * (oid *) BUNhloc(li, p1);

				h = (ptr) &o;
				t = BUNtail(li, p1);

				if (o >= rl && o < rh) {
					BUN w = off + (o - rl);

					t2 = BUNtail(ri, w);
					if (EQUALs(t, t2)) {
						HITdiff(h, t);
						continue;
					}
				}
				MISSdiff(h, t);
			}
		}
	} else {
		switch(ATOMstorage(r->htype)) {
		case TYPE_bte:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _bte, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, bte, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_bte(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,bte))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _bte, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			bte *h = (bte*)BUNhloc(li, BUNfirst(l));
			bte *rh = (bte*)BUNhloc(ri, 0);
			bte *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _bte, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (bte*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			bte *h = (bte*)BUNhloc(li, 0);
			bte *rh = (bte*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _bte, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _bte, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _bte, simple_CMP(h,h2,bte), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,bte)) > 0) {
				if ((++p2) >= q2)
					goto endloc_bte;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,bte)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,bte)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_bte:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 727 "gdk_setop.mx"

		case TYPE_sht:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _sht, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, sht, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_sht(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,sht))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _sht, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			sht *h = (sht*)BUNhloc(li, BUNfirst(l));
			sht *rh = (sht*)BUNhloc(ri, 0);
			sht *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _sht, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (sht*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			sht *h = (sht*)BUNhloc(li, 0);
			sht *rh = (sht*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _sht, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _sht, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _sht, simple_CMP(h,h2,sht), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,sht)) > 0) {
				if ((++p2) >= q2)
					goto endloc_sht;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,sht)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,sht)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_sht:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 729 "gdk_setop.mx"

		case TYPE_int:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _int, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, int, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_int(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,int))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _int, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			int *h = (int*)BUNhloc(li, BUNfirst(l));
			int *rh = (int*)BUNhloc(ri, 0);
			int *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _int, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (int*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			int *h = (int*)BUNhloc(li, 0);
			int *rh = (int*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _int, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _int, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _int, simple_CMP(h,h2,int), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,int)) > 0) {
				if ((++p2) >= q2)
					goto endloc_int;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,int)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,int)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_int:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 731 "gdk_setop.mx"

		case TYPE_flt:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _flt, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, flt, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_flt(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,flt))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _flt, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			flt *h = (flt*)BUNhloc(li, BUNfirst(l));
			flt *rh = (flt*)BUNhloc(ri, 0);
			flt *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _flt, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (flt*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			flt *h = (flt*)BUNhloc(li, 0);
			flt *rh = (flt*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _flt, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _flt, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _flt, simple_CMP(h,h2,flt), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,flt)) > 0) {
				if ((++p2) >= q2)
					goto endloc_flt;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,flt)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,flt)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_flt:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 733 "gdk_setop.mx"

		case TYPE_dbl:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _dbl, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, dbl, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_dbl(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,dbl))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _dbl, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			dbl *h = (dbl*)BUNhloc(li, BUNfirst(l));
			dbl *rh = (dbl*)BUNhloc(ri, 0);
			dbl *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _dbl, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (dbl*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			dbl *h = (dbl*)BUNhloc(li, 0);
			dbl *rh = (dbl*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _dbl, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _dbl, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _dbl, simple_CMP(h,h2,dbl), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,dbl)) > 0) {
				if ((++p2) >= q2)
					goto endloc_dbl;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,dbl)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,dbl)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_dbl:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 735 "gdk_setop.mx"

		case TYPE_lng:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _lng, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, lng, s];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_lng(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,lng))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _lng, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			lng *h = (lng*)BUNhloc(li, BUNfirst(l));
			lng *rh = (lng*)BUNhloc(ri, 0);
			lng *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _lng, s][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (lng*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
					    EQUALs(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			lng *h = (lng*)BUNhloc(li, 0);
			lng *rh = (lng*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _lng, s]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALs(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _lng, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _lng, simple_CMP(h,h2,lng), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,lng)) > 0) {
				if ((++p2) >= q2)
					goto endloc_lng;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,lng)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,lng)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_lng:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 737 "gdk_setop.mx"

		default:
			if (r->hvarsized) {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, var, var, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffvar: hashcheck[diff, var, var, _str_hv, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffvar: hashcheck[diff, var, var, var, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffvar: mergecheck[diff, var, var, ((*merge)(h,h2)), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhvar(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhvar(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endvarvar;
				h2 = BUNhvar(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhvar(ri, pb);
								if (((*merge)(h,h2))) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endvarvar:;
	ENDdiff(BUNhvar(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 740 "gdk_setop.mx"

			} else {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, loc, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _str_hv, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, loc, s];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALs(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, loc, ((*merge)(h,h2)), s];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endlocloc;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALs(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (((*merge)(h,h2))) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endlocloc:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 742 "gdk_setop.mx"

			}
		}
	}
	return b;
  bunins_failed:
	BBPreclaim(b);
	return NULL;
}


#line 755 "gdk_setop.mx"


#line 615 "gdk_setop.mx"
static BAT*
BATins_kintersect(BAT *bn, BAT *l, BAT *r)
{
	int hash = TRUE, (*cmp)(const void *, const void *), (*merge)(const void *, const void *) = NULL;
	ptr hnil, tnil;
	BAT *b = bn;

	/* determine how to do the intersect */
	if (BAThordered(l) & BAThordered(r)) {
		hash = FALSE;
	}
#if FLIPk
	else {
		int flip = BATtordered(l) & BATtordered(r);

		if (flip) {
			hash = FALSE;
		} else {
			flip = r->H->hash == NULL && r->T->hash != NULL;
		}
		if (flip) {
			r = BATmirror(r);
			l = BATmirror(l);
			bn = BATmirror(bn);
		}
	}
#endif
	merge = BATatoms[l->htype].atomCmp;
	cmp = BATatoms[l->ttype].atomCmp;
	hnil = ATOMnilptr(l->htype);
	tnil = ATOMnilptr(l->ttype);
	(void) cmp;
	(void) tnil;
	(void) hnil;

	if (BAThdense(r)) {
		/* voidcheck */
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BUN p1 = BUNfirst(r), q1 = BUNlast(r);
		oid rl = * (oid *) BUNhead(ri, p1);
		oid rh = rl + BATcount(r);
		ptr h, t = NULL, t2 = NULL;

		(void) t2;

		ALGODEBUG fprintf(stderr, "#BATins_kintersect: voidcheck[k, intersect];\n");
		if (BAThdense(l)) {
			oid ll = * (oid *) BUNhead(li, (p1 = BUNfirst(l)));
			oid lh = ll + BATcount(l);
			BUN hit_start = (q1 = BUNlast(l)), hit_end = q1, w = BUNfirst(r);
			BUN off = p1;

			h = (ptr) &ll;

			if (rl >= ll && rl < lh) {
				hit_start = off + (rl - ll);
			} else if (rl < ll && rh > ll) {
				hit_start = p1;
				w += (ll - rl);
			}
			if (rh >= ll && rh < lh) {
				hit_end = off + (rh - ll);
			}
			while(p1 < hit_start) {
				t = BUNtail(li, p1);
				MISSintersect(h, t);
				ll++;
				p1++;
			}
			while(p1 < hit_end) {
				t = BUNtail(li, p1);
				t2 = BUNtail(ri, w);
				if (EQUALk(t, t2)) {
					HITintersect(h, t);
				} else {
					MISSintersect(h, t);
				}
				ll++;
				p1++;
				w++;
			}
			while (p1 < q1) {
				t = BUNtail(li, p1);
				MISSintersect(h, t);
				ll++;
				p1++;
			}
		} else {
			BUN off = p1;

			BATloop(l, p1, q1) {
				oid o = * (oid *) BUNhloc(li, p1);

				h = (ptr) &o;
				t = BUNtail(li, p1);

				if (o >= rl && o < rh) {
					BUN w = off + (o - rl);

					t2 = BUNtail(ri, w);
					if (EQUALk(t, t2)) {
						HITintersect(h, t);
						continue;
					}
				}
				MISSintersect(h, t);
			}
		}
	} else {
		switch(ATOMstorage(r->htype)) {
		case TYPE_bte:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _bte, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, bte, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_bte(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,bte))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _bte, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			bte *h = (bte*)BUNhloc(li, BUNfirst(l));
			bte *rh = (bte*)BUNhloc(ri, 0);
			bte *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _bte, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (bte*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			bte *h = (bte*)BUNhloc(li, 0);
			bte *rh = (bte*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _bte, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _bte, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _bte, simple_CMP(h,h2,bte), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,bte)) > 0) {
				if ((++p2) >= q2)
					goto endloc_bte;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,bte)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,bte)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_bte:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 727 "gdk_setop.mx"

		case TYPE_sht:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _sht, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, sht, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_sht(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,sht))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _sht, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			sht *h = (sht*)BUNhloc(li, BUNfirst(l));
			sht *rh = (sht*)BUNhloc(ri, 0);
			sht *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _sht, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (sht*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			sht *h = (sht*)BUNhloc(li, 0);
			sht *rh = (sht*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _sht, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _sht, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _sht, simple_CMP(h,h2,sht), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,sht)) > 0) {
				if ((++p2) >= q2)
					goto endloc_sht;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,sht)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,sht)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_sht:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 729 "gdk_setop.mx"

		case TYPE_int:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _int, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, int, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_int(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,int))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _int, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			int *h = (int*)BUNhloc(li, BUNfirst(l));
			int *rh = (int*)BUNhloc(ri, 0);
			int *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _int, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (int*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			int *h = (int*)BUNhloc(li, 0);
			int *rh = (int*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _int, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _int, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _int, simple_CMP(h,h2,int), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,int)) > 0) {
				if ((++p2) >= q2)
					goto endloc_int;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,int)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,int)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_int:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 731 "gdk_setop.mx"

		case TYPE_flt:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _flt, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, flt, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_flt(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,flt))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _flt, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			flt *h = (flt*)BUNhloc(li, BUNfirst(l));
			flt *rh = (flt*)BUNhloc(ri, 0);
			flt *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _flt, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (flt*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			flt *h = (flt*)BUNhloc(li, 0);
			flt *rh = (flt*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _flt, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _flt, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _flt, simple_CMP(h,h2,flt), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,flt)) > 0) {
				if ((++p2) >= q2)
					goto endloc_flt;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,flt)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,flt)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_flt:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 733 "gdk_setop.mx"

		case TYPE_dbl:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _dbl, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, dbl, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_dbl(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,dbl))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _dbl, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			dbl *h = (dbl*)BUNhloc(li, BUNfirst(l));
			dbl *rh = (dbl*)BUNhloc(ri, 0);
			dbl *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _dbl, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (dbl*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			dbl *h = (dbl*)BUNhloc(li, 0);
			dbl *rh = (dbl*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _dbl, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _dbl, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _dbl, simple_CMP(h,h2,dbl), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,dbl)) > 0) {
				if ((++p2) >= q2)
					goto endloc_dbl;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,dbl)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,dbl)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_dbl:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 735 "gdk_setop.mx"

		case TYPE_lng:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, _lng, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, lng, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_lng(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,lng))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _lng, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			lng *h = (lng*)BUNhloc(li, BUNfirst(l));
			lng *rh = (lng*)BUNhloc(ri, 0);
			lng *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _lng, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (lng*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITintersect(h+p1, b);
					} else {
						DMISSintersect(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			lng *h = (lng*)BUNhloc(li, 0);
			lng *rh = (lng*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_intersectloc: directcheck[intersect, loc, loc, _lng, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITintersect_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSintersect_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _lng, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, _lng, simple_CMP(h,h2,lng), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,lng)) > 0) {
				if ((++p2) >= q2)
					goto endloc_lng;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,lng)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,lng)) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endloc_lng:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 737 "gdk_setop.mx"

		default:
			if (r->hvarsized) {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, var, var, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectvar: hashcheck[intersect, var, var, _str_hv, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectvar: hashcheck[intersect, var, var, var, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectvar: mergecheck[intersect, var, var, ((*merge)(h,h2)), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhvar(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhvar(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endvarvar;
				h2 = BUNhvar(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhvar(ri, pb);
								if (((*merge)(h,h2))) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endvarvar:;
	ENDintersect(BUNhvar(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 740 "gdk_setop.mx"

			} else {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectpos: hashcheck[intersect, pos, loc, loc, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, _str_hv, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: hashcheck[intersect, loc, loc, loc, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITintersect(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSintersect(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_intersectloc: mergecheck[intersect, loc, loc, ((*merge)(h,h2)), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endlocloc;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITintersect(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSintersect(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (((*merge)(h,h2))) {
									MISSintersect(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSintersect(h, t);
		}
  endlocloc:;
	ENDintersect(BUNhloc(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 742 "gdk_setop.mx"

			}
		}
	}
	return b;
  bunins_failed:
	BBPreclaim(b);
	return NULL;
}


#line 756 "gdk_setop.mx"


#line 615 "gdk_setop.mx"
static BAT*
BATins_kdiff(BAT *bn, BAT *l, BAT *r)
{
	int hash = TRUE, (*cmp)(const void *, const void *), (*merge)(const void *, const void *) = NULL;
	ptr hnil, tnil;
	BAT *b = bn;

	/* determine how to do the intersect */
	if (BAThordered(l) & BAThordered(r)) {
		hash = FALSE;
	}
#if FLIPk
	else {
		int flip = BATtordered(l) & BATtordered(r);

		if (flip) {
			hash = FALSE;
		} else {
			flip = r->H->hash == NULL && r->T->hash != NULL;
		}
		if (flip) {
			r = BATmirror(r);
			l = BATmirror(l);
			bn = BATmirror(bn);
		}
	}
#endif
	merge = BATatoms[l->htype].atomCmp;
	cmp = BATatoms[l->ttype].atomCmp;
	hnil = ATOMnilptr(l->htype);
	tnil = ATOMnilptr(l->ttype);
	(void) cmp;
	(void) tnil;
	(void) hnil;

	if (BAThdense(r)) {
		/* voidcheck */
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BUN p1 = BUNfirst(r), q1 = BUNlast(r);
		oid rl = * (oid *) BUNhead(ri, p1);
		oid rh = rl + BATcount(r);
		ptr h, t = NULL, t2 = NULL;

		(void) t2;

		ALGODEBUG fprintf(stderr, "#BATins_kdiff: voidcheck[k, diff];\n");
		if (BAThdense(l)) {
			oid ll = * (oid *) BUNhead(li, (p1 = BUNfirst(l)));
			oid lh = ll + BATcount(l);
			BUN hit_start = (q1 = BUNlast(l)), hit_end = q1, w = BUNfirst(r);
			BUN off = p1;

			h = (ptr) &ll;

			if (rl >= ll && rl < lh) {
				hit_start = off + (rl - ll);
			} else if (rl < ll && rh > ll) {
				hit_start = p1;
				w += (ll - rl);
			}
			if (rh >= ll && rh < lh) {
				hit_end = off + (rh - ll);
			}
			while(p1 < hit_start) {
				t = BUNtail(li, p1);
				MISSdiff(h, t);
				ll++;
				p1++;
			}
			while(p1 < hit_end) {
				t = BUNtail(li, p1);
				t2 = BUNtail(ri, w);
				if (EQUALk(t, t2)) {
					HITdiff(h, t);
				} else {
					MISSdiff(h, t);
				}
				ll++;
				p1++;
				w++;
			}
			while (p1 < q1) {
				t = BUNtail(li, p1);
				MISSdiff(h, t);
				ll++;
				p1++;
			}
		} else {
			BUN off = p1;

			BATloop(l, p1, q1) {
				oid o = * (oid *) BUNhloc(li, p1);

				h = (ptr) &o;
				t = BUNtail(li, p1);

				if (o >= rl && o < rh) {
					BUN w = off + (o - rl);

					t2 = BUNtail(ri, w);
					if (EQUALk(t, t2)) {
						HITdiff(h, t);
						continue;
					}
				}
				MISSdiff(h, t);
			}
		}
	} else {
		switch(ATOMstorage(r->htype)) {
		case TYPE_bte:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _bte, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, bte, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_bte(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,bte))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _bte, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			bte *h = (bte*)BUNhloc(li, BUNfirst(l));
			bte *rh = (bte*)BUNhloc(ri, 0);
			bte *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _bte, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (bte*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			bte *h = (bte*)BUNhloc(li, 0);
			bte *rh = (bte*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _bte, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_bte(H, h+p1);
					if (d[i] != 0 && bte_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _bte, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,bte)) /* check for not-nil (nils don't match anyway) */
			HASHloop_bte(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,bte) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _bte, simple_CMP(h,h2,bte), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,bte)) > 0) {
				if ((++p2) >= q2)
					goto endloc_bte;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,bte)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,bte)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_bte:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 727 "gdk_setop.mx"

		case TYPE_sht:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _sht, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, sht, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_sht(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,sht))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _sht, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			sht *h = (sht*)BUNhloc(li, BUNfirst(l));
			sht *rh = (sht*)BUNhloc(ri, 0);
			sht *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _sht, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (sht*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			sht *h = (sht*)BUNhloc(li, 0);
			sht *rh = (sht*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _sht, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_sht(H, h+p1);
					if (d[i] != 0 && sht_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _sht, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,sht)) /* check for not-nil (nils don't match anyway) */
			HASHloop_sht(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,sht) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _sht, simple_CMP(h,h2,sht), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,sht)) > 0) {
				if ((++p2) >= q2)
					goto endloc_sht;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,sht)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,sht)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_sht:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 729 "gdk_setop.mx"

		case TYPE_int:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _int, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, int, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_int(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,int))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _int, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			int *h = (int*)BUNhloc(li, BUNfirst(l));
			int *rh = (int*)BUNhloc(ri, 0);
			int *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _int, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (int*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			int *h = (int*)BUNhloc(li, 0);
			int *rh = (int*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _int, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_int(H, h+p1);
					if (d[i] != 0 && int_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _int, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,int)) /* check for not-nil (nils don't match anyway) */
			HASHloop_int(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,int) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _int, simple_CMP(h,h2,int), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,int)) > 0) {
				if ((++p2) >= q2)
					goto endloc_int;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,int)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,int)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_int:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 731 "gdk_setop.mx"

		case TYPE_flt:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _flt, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, flt, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_flt(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,flt))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _flt, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			flt *h = (flt*)BUNhloc(li, BUNfirst(l));
			flt *rh = (flt*)BUNhloc(ri, 0);
			flt *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _flt, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (flt*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			flt *h = (flt*)BUNhloc(li, 0);
			flt *rh = (flt*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _flt, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_flt(H, h+p1);
					if (d[i] != 0 && flt_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _flt, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,flt)) /* check for not-nil (nils don't match anyway) */
			HASHloop_flt(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,flt) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _flt, simple_CMP(h,h2,flt), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,flt)) > 0) {
				if ((++p2) >= q2)
					goto endloc_flt;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,flt)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,flt)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_flt:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 733 "gdk_setop.mx"

		case TYPE_dbl:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _dbl, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, dbl, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_dbl(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,dbl))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _dbl, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			dbl *h = (dbl*)BUNhloc(li, BUNfirst(l));
			dbl *rh = (dbl*)BUNhloc(ri, 0);
			dbl *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _dbl, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (dbl*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			dbl *h = (dbl*)BUNhloc(li, 0);
			dbl *rh = (dbl*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _dbl, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_dbl(H, h+p1);
					if (d[i] != 0 && dbl_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _dbl, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,dbl)) /* check for not-nil (nils don't match anyway) */
			HASHloop_dbl(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,dbl) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _dbl, simple_CMP(h,h2,dbl), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,dbl)) > 0) {
				if ((++p2) >= q2)
					goto endloc_dbl;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,dbl)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,dbl)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_dbl:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 735 "gdk_setop.mx"

		case TYPE_lng:
			
#line 601 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, _lng, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 602 "gdk_setop.mx"

	} else if (hash) {
		if (BATcount(r) < DIRECT_MAX) {
			
#line 493 "gdk_setop.mx"
	BUN p1, q1;
	int i;
	ptr h, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	sht d[DIRECT_MAX];
	Hash hs, *H = &hs;
	int collision = 0;

	H -> mask = DIRECT_MAX-1;
	H -> type = BAThtype(l);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, lng, k];\n");

	assert(l->htype == r->htype && r->htype != TYPE_void);

	memset(d, 0, sizeof(d));
	BATloop(r, p1, q1) {
		h = BUNhloc(ri,p1);
		i = (int) hash_lng(H, h);
		/* collision or check for not-nil (nils don't match anyway) */
		if (d[i] != 0 || !(simple_CMP(h,h2,lng))) {
			collision = 1;
			break;
		}
		d[i] = ((sht)p1)+1;
	}
	if (collision) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _lng, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 521 "gdk_setop.mx"

	} else {
		if (!l->ttype && l->tseqbase != oid_nil) {
			oid b = l->tseqbase, *t = &b;
			lng *h = (lng*)BUNhloc(li, BUNfirst(l));
			lng *rh = (lng*)BUNhloc(ri, 0);
			lng *bnh;
			oid *bnt;
			BUN o = BUNfirst(bn);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _lng, k][void tail]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = 0;
			q1 = BATcount(l);
			while(p1 < q1) {
				BUN r1;
				if (p1 + 1 > BATcapacity(bn)){	
					BATsetcount(bn, o);
					if (BATextend(bn, BATgrows(bn)) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				bnh = (lng*)Hloc(bn,0);
				bnt = (oid*)Tloc(bn,0);
				for (; p1<r1; p1++, b++){ 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
					    EQUALk(t, BUNtail(ri, d[i]-1))) {
						DHITdiff(h+p1, b);
					} else {
						DMISSdiff(h+p1, b);
					}
				}
			}
			BATsetcount(bn, o);
			(void)t;
		} else { 
			lng *h = (lng*)BUNhloc(li, 0);
			lng *rh = (lng*)BUNhloc(ri, 0);

			ALGODEBUG fprintf(stderr, "#BATins_diffloc: directcheck[diff, loc, loc, _lng, k]; " BUNFMT " " BUNFMT "\n", BATcount(l), BATcount(r));
			p1 = BUNfirst(l);
			q1 = BUNlast(l);
			while(p1 < q1) {
				BUN r1;
				if (BUNlast(bn) + 1 > BATcapacity(bn)){	
					if (BATextend(bn, BATcapacity(bn)+65536) == NULL)
						goto bunins_failed; 
				}
				r1 = p1 + BATcapacity(bn) - BUNlast(bn);
				if (r1 > q1) r1 = q1;
				for (; p1<r1; p1++) { 
					i = (int) hash_lng(H, h+p1);
					if (d[i] != 0 && lng_EQ(h+p1, rh+d[i]-1) &&
				    	    EQUALk(BUNtail(li,p1), BUNtail(ri, d[i]-1))) {
						HITdiff_nocheck(h+p1, BUNtail(li, p1));
					} else {
						MISSdiff_nocheck(h+p1, BUNtail(li, p1));
					}
				}
			}
		}
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 605 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _lng, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (simple_CMP(h,h2,lng)) /* check for not-nil (nils don't match anyway) */
			HASHloop_lng(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the simple_CMP(h,h2,lng) check doesn't use the h2 */



#line 607 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, _lng, simple_CMP(h,h2,lng), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = simple_CMP(h,h2,lng)) > 0) {
				if ((++p2) >= q2)
					goto endloc_lng;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (simple_CMP(h,h2,lng)) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (simple_CMP(h,h2,lng)) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endloc_lng:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 610 "gdk_setop.mx"

	}
	break;



#line 737 "gdk_setop.mx"

		default:
			if (r->hvarsized) {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, var, var, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffvar: hashcheck[diff, var, var, _str_hv, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffvar: hashcheck[diff, var, var, var, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhvar(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloopvar(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffvar: mergecheck[diff, var, var, ((*merge)(h,h2)), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhvar(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhvar(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endvarvar;
				h2 = BUNhvar(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhvar(ri, pb);
								if (((*merge)(h,h2))) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endvarvar:;
	ENDdiff(BUNhvar(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 740 "gdk_setop.mx"

			} else {
				
#line 587 "gdk_setop.mx"
	if (BAThdense(l)) {
		
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffpos: hashcheck[diff, pos, loc, loc, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhpos(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (TRUE) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the TRUE check doesn't use the h2 */



#line 588 "gdk_setop.mx"

	} else if (hash) {
		if (l->htype == TYPE_str && l->H->vheap->hashash) {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, _str_hv, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHloop_str_hv(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 591 "gdk_setop.mx"

		} else {
			
#line 451 "gdk_setop.mx"
	BUN p1, q1;
	int ins;
	BUN s2;
	ptr h, t, h2 = hnil;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: hashcheck[diff, loc, loc, loc, k];\n");
	if (BATprepareHash(r)) {
		goto bunins_failed;
	}
	BATloop(l, p1, q1) {
		h = BUNhloc(li, p1);
		t = BUNtail(li, p1);
		ins = TRUE;
		if (((*merge)(h,h2))) /* check for not-nil (nils don't match anyway) */
			HASHlooploc(ri, r->H->hash, s2, h) {
				if (EQUALk(t, BUNtail(ri, s2))) {
					HITdiff(h, t);
					ins = FALSE;
					break;
				}
			}
		if (!ins)
			continue;
		MISSdiff(h, t);
	}
	(void)h2; /* in some cases the ((*merge)(h,h2)) check doesn't use the h2 */



#line 593 "gdk_setop.mx"

		}
	} else {
		
#line 403 "gdk_setop.mx"
	BUN p1 = BUNfirst(l), p2 = BUNfirst(r);
	BUN q1 = BUNlast(l),  q2 = BUNlast(r);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	ALGODEBUG fprintf(stderr, "#BATins_diffloc: mergecheck[diff, loc, loc, ((*merge)(h,h2)), k];\n");
	if (p2 < q2)
		BATloop(l, p1, q1) {
			ptr  h = BUNhloc(li, p1);
			ptr  t = BUNtail(li, p1);
			ptr h2 = BUNhloc(ri, p2);
			int c;
			while ((c = ((*merge)(h,h2))) > 0) {
				if ((++p2) >= q2)
					goto endlocloc;
				h2 = BUNhloc(ri, p2);
			}
			if (c == 0) {
				h2 = hnil;
				if (((*merge)(h,h2))) { /* check for not-nil (nils don't match anyway) */
					BUN pb = p2;
					int done = FALSE;

					for (;!done;) {
						if (EQUALk(t, BUNtail(ri, pb))) {
							HITdiff(h, t);
							done = TRUE;
						} else {
							if ((++pb) >= q2) {
								MISSdiff(h, t);
								done = TRUE;
							} else {
								h2 = BUNhloc(ri, pb);
								if (((*merge)(h,h2))) {
									MISSdiff(h, t);
									done = TRUE;
								}
							}
						}
					}
					continue;
				}
			}
			MISSdiff(h, t);
		}
  endlocloc:;
	ENDdiff(BUNhloc(li, p1), BUNtail(li, p1));


#line 596 "gdk_setop.mx"

	}
	break;



#line 742 "gdk_setop.mx"

			}
		}
	}
	return b;
  bunins_failed:
	BBPreclaim(b);
	return NULL;
}


#line 757 "gdk_setop.mx"



static BAT *
diff_intersect(BAT *l, BAT *r, int diff, int set)
{
	BUN smaller;
	BAT *bn;

	ERRORcheck(l == NULL, "diff_intersect: left is null");
	ERRORcheck(r == NULL, "diff_intersect: right is null");
	ERRORcheck(TYPEerror(BAThtype(l), BAThtype(r)), "diff_intersect: incompatible head-types");
	if (set)
		ERRORcheck(TYPEerror(BATttype(l), BATttype(r)), "diff_intersect: incompatible tail-types");

	if (BATcount(r) == 0) {
		return diff ? BATcopy(l, l->htype, l->ttype, FALSE) : BATclone(l, 10);
	} else if (BATcount(l) == 0) {
		return BATclone(l, 10);
	}
	smaller = BATcount(l);
	if (!diff && BATcount(r) < smaller)
		smaller = BATcount(r);
	bn = BATnew(BAThtype(l), BATttype(l), MAX(smaller,BATTINY));
	if (bn == NULL)
		return NULL;

	/* fill result bat bn */
	if (set) {
		if (diff) {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_sdiff(bn, l, r);\n");
			bn = BATins_sdiff(bn, l, r);
		} else {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_sintersect(bn, l, r);\n");
			bn = BATins_sintersect(bn, l, r);
		}
	} else {
		if (diff) {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_kdiff(bn, l, r);\n");
			bn = BATins_kdiff(bn, l, r);
		} else {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_kintersect(bn, l, r);\n");
			bn = BATins_kintersect(bn, l, r);
		}
	}
	if (bn == NULL)
		return NULL;

	/* propagate alignment info */
	if (BATcount(bn) == BATcount(l)) {
		ALIGNset(bn, l);
	}
	if (!diff &&
	    BAThordered(l) & BAThordered(r) &&
	    l->hkey &&
	    BATcount(bn) == BATcount(r)) {
		ALIGNsetH(bn, r);
	}
	bn->hsorted = BAThordered(l);
	bn->hrevsorted = BAThrevordered(l);
	bn->tsorted = BATtordered(l);
	bn->trevsorted = BATtrevordered(l);
	if (BATcount(bn)) {
		BATkey(bn, BAThkey(l));
		BATkey(BATmirror(bn), BATtkey(l));
	} else {
		BATkey(bn, TRUE);
		BATkey(BATmirror(bn), TRUE);
	}
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = l->T->nonil;
	return bn;
}

BAT *
BATsdiff(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 1, 1);
}

BAT *
BATsintersect(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 0, 1);
}

BAT *
BATkdiff(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 1, 0);
}

BAT *
BATkintersect(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 0, 0);
}

/*
 * @+ Union
 * Union also consists of two versions: BATkunion(l,r), which
 * unites with double elimination over the head column only, and
 * BATsunion(l,r), that looks at both columns. Their
 * implementation uses the s/kdiff() and s/kunique() code for efficient
 * double elimination.
 */

static BAT *
BATunion(BAT *l, BAT *r, int set)
{
	int hdisjunct, tdisjunct;
	BAT *bn, *b;
	BUN p,q;
	BATiter li, ri;
	int ht, tt;

	BATcompatible(l, r);
	if (BATcount(l) == 0) {
		b = l;
		l = r;
		r = b;
	}
	if (BATcount(r) == 0) {
		return BATcopy(l, l->htype, l->ttype, FALSE);
	}

	b = NULL;
	li = bat_iterator(l);
 	ri = bat_iterator(r);
	hdisjunct = BAThordered(r) & BAThordered(l) &&
		    ATOMcmp(l->htype, BUNhead(li, BUNlast(l) - 1), BUNhead(ri, BUNfirst(r))) < 0;
	tdisjunct = BATtordered(r) & BATtordered(l) &&
		    ATOMcmp(l->ttype, BUNtail(li, BUNlast(l) - 1), BUNtail(ri, BUNfirst(r))) < 0;

	if (!hdisjunct) {
		b = r;
		ri.b = r = set ? BATsdiff(r, l) : BATkdiff(r, l);
		if (r == NULL) {
			return NULL;
		}
	}

	if (BATcount(r) == 0) {
		if (b)
			BBPreclaim(r);
		return BATcopy(l, l->htype, l->ttype, FALSE);
	}

	ht = l->htype;
	tt = l->ttype;
	if (ht == TYPE_void && l->hseqbase != oid_nil)
		ht = TYPE_oid;
	if (tt == TYPE_void && l->tseqbase != oid_nil)
		tt = TYPE_oid;
	bn = BATcopy(l, ht, tt, TRUE);
	if (bn == NULL) {
		if (b)
			BBPreclaim(r);
		return NULL;
	}
	BATloop(r, p, q) {
		bunfastins(bn, BUNhead(ri, p), BUNtail(ri, p));
	}
	if (!BAThdense(l) || !BAThdense(r) ||
	    * (oid *) BUNhead(li, BUNlast(l) - 1) + 1 != * (oid *) BUNhead(ri, BUNfirst(r))) {
		bn->hseqbase = oid_nil;
		bn->hdense = 0;
	}
	if (!BATtdense(l) || !BATtdense(r) ||
	    * (oid *) BUNtail(li, BUNlast(l) - 1) + 1 != * (oid *) BUNtail(ri, BUNfirst(r))) {
		bn->tseqbase = oid_nil;
		bn->tdense = 0;
	}
	bn->H->nonil = l->H->nonil & r->H->nonil;
	bn->T->nonil = l->T->nonil & r->T->nonil;
	bn->H->nil = l->H->nil | r->H->nil;
	bn->T->nil = l->T->nil | r->T->nil;
	if (b) {
		BBPreclaim(r);
		r = b;
	}
	HASHdestroy(bn);

	bn->hsorted = hdisjunct;
	bn->hrevsorted = 0;
	bn->tsorted = tdisjunct;
	bn->trevsorted = 0;
	bn->talign = bn->halign = 0;
	if (!r->hkey)
		BATkey(bn, FALSE);
	if (set && bn->hkey && hdisjunct == FALSE)
		BATkey(bn, FALSE);
	BATkey(BATmirror(bn), tdisjunct && BATtkey(l) && BATtkey(r));

	return bn;
  bunins_failed:
	BBPreclaim(bn);
	if (b)
		BBPreclaim(r);
	return NULL;
}

BAT *
BATsunion(BAT *l, BAT *r)
{
	return BATunion(l, r, 1);
}

BAT *
BATkunion(BAT *l, BAT *r)
{
	return BATunion(l, r, 0);
}

