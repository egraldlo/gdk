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
/*
 * 三万行代码的大文件，主要是relational 的操作，其中有各种join算法
 * 这里面的算法包括：
 * merge-join : BATmergejoin
 * hashjoin   : BAThashjoin
 * fetchjoin  : 这是一种什么join
 * thetajoin  : >=,<=,>,<
 * semijoin   : BATsemijoin
 * antijoin   : 不等连接
 * */
#line 23 "gdk_relop.mx"
/*
 * @a M. L. Kersten, P. Boncz, S. Manegold
 * @* BAT relational operators
 * The basic relational operators are implemented for BATs.
 * Particular attention has been paid to speed-up processing
 * joins, such that navigational access and object re-assembly
 * are not being harmed too much.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define SAMPLE_TRESHOLD_LOG 17
#define SAMPLE_SLICE_SIZE 1000

static BAT *BATnlthetajoin(BAT *l, BAT *r, int op, BUN estimate);

/*
 * @+ Join Algorithms
 * All join related operations have the same prelude to check
 * domain compatibility and to creates the BAT to hold the result.
 * 所有的join算法都有相同的前戏，就是检查兼容性和创建BAT去hold结果
 * We do some dynamic effort to estimate the result size. Good
 * estimates enhance performance and reduce the memory hunger of the join.
 * Method: we sample on l, and join on the whole r. This macro is called by
 * the physical join algorithms, hence we already decided on the algorithm
 * and join method, so the initial costs on r (e.g. hash creation) would have
 * to be paid anyway, and are reused later in the real join phase.
 *
 * Sampling was made more robust by using a logarithmic number of slices
 * taken at equal-spaced intervals across l. The results are then analyzed
 * and checked for outliers. If outliers are present, a real sample is taken
 * and executed with the generic join algorithm to obtain an better estimate.
 *
 * On small joins we just assume 1-N joins with a limited (=3) hit rate.
 */


#line 180 "gdk_relop.mx"
/*
 * @- Merge join
 * In the case that both join columns are ordered, we can do a merging
 * (BATmergejoin). The merge is opportunistic in that it tries to do
 * merge between l and r, but if for a long time no matching tuples are
 * found in r, it uses binary search. It also allows joining of an
 * unsorted l with a sorted r; in that case it always uses binary search.
 */
#define BATPERC(lfirst,lcur,lend,rfirst,rcur,rend)\
		((((dbl) (lend-lfirst))*((dbl) (rend-rfirst))) / \
		 MAX(1,((lcur-lfirst)*((dbl) (rend-rfirst))+(dbl) (rcur-rfirst))))

#define bunfastins_limit(b, h, t, limit, percdone)			\
	do {								\
                register BUN _p = BUNlast(b);				\
		if (_p == BUN_MAX) /* reached maximum, can't do more */	\
			goto bunins_done;				\
                if (_p + 1 > BATcapacity(b)) {				\
                        if (limit) {					\
				*limit = (BUN) (b->batCount * (percdone)); \
                              goto bunins_done;				\
			}						\
                        if (BATextend(b, BATgrows(b)) == NULL)		\
                                goto bunins_failed;			\
                }							\
                hfastins_nocheck(b, _p, h, Hsize(b));			\
                tfastins_nocheck(b, _p, t, Tsize(b));			\
                (b)->batCount++;					\
        } while (0)


#line 289 "gdk_relop.mx"
/* serves both normal equi-join (nil_on_miss==NULL) and outerjoin
 * (nil_on_miss=nil) */
static BAT *
mergejoin(BAT *l, BAT *r, BAT *bn, ptr nil_on_miss, BUN estimate, BUN *limit)
{
	ptr nil = ATOMnilptr(r->htype);
	int r_scan = -1;	/* no scanning in r */
	BAT *rr = BATmirror(r);
	BUN l_last, r_last;	/* last BUN of the BAT */
	BUN l_start, r_start;	/* start of current chunk  */
	BUN l_end, r_end;	/* end of current chunk */
	int l_key = l->tkey;
	int r_key = r->hkey;
	BUN r_cur, r_lim;
	int loc, var, hasnils = 0;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	if (BATtordered(l)) {
		BUN i;
		int logr = 4;

		/* 4*log2(r.count) = estimation of the cost of binary
		 * search in units of scan comparisons */
		for (i = BATcount(r); i > 0; logr++)
			i >>= 1;
		r_scan = logr;	/* opportunistic scan window in r */
	}
	if (!BAThordered(r)) {
		GDKerror("mergejoin: right input is not sorted.\n");
		return NULL;
	}
	if (bn == NULL) {
		
#line 170 "gdk_relop.mx"
	{
		BUN _estimate = estimate;

		
#line 64 "gdk_relop.mx"
	if ( _estimate == BUN_NONE) {
		BUN _lcount = BATcount(l);
		BUN _rcount = BATcount(r);
		BUN _slices = 0;

		/* limit estimate with simple bounds first; only spend
		 * effort if the join result might be big */
		if (JOIN_EQ == JOIN_EQ) {
			if (l->tkey)
				 _estimate = r->hkey ? MIN(_rcount, _lcount) : _rcount;
			else if (r->hkey)
				 _estimate = _lcount;
		}
		if ( _estimate == BUN_NONE) {
			BUN _heuristic = MIN(_lcount, _rcount);

			if (_heuristic <= BUN_MAX / 3) {
				_heuristic *= 3;
				if (_heuristic <= (1 << SAMPLE_TRESHOLD_LOG))
					 _estimate = _heuristic;
			}
		}
		if ( _estimate == BUN_NONE) {
			BUN _idx;

			for (_idx = _lcount; _idx > 0; _idx >>= 1)
				_slices++;
		}
		if (_slices > SAMPLE_TRESHOLD_LOG) {
			/* use cheapo sampling by taking a number of
			 * slices and joining those with the algo */
			BUN _idx = 0, _tot = 0, _step, _lo, _avg, _sample, *_cnt;
			BAT *_tmp1 = l, *_tmp2, *_tmp3 = NULL;

			_step = _lcount / (_slices -= SAMPLE_TRESHOLD_LOG);
			_sample = _slices * SAMPLE_SLICE_SIZE;
			_cnt = GDKmalloc(_slices * sizeof(BUN));
			if (_cnt == NULL)
				return NULL;
			for (_lo = 0; _idx < _slices; _lo += _step) {
				BUN _size = 0, _hi = _lo + SAMPLE_SLICE_SIZE;

				l = BATslice(_tmp1, _lo, _hi);	/* slice keeps all parent properties */
				if (l == NULL) {
					GDKfree(_cnt);
					return NULL;
				}
				_tmp2 =  mergejoin(l,r,NULL,nil_on_miss,BUN_NONE,NULL);	/*  mergejoin(l,r,NULL,nil_on_miss,BUN_NONE,NULL) = e.g. BATXjoin(l,r) */
				if (_tmp2) {
					_size = BATcount(_tmp2);
					BBPreclaim(_tmp2);
				}
				_tot += (_cnt[_idx++] = _size);
				BBPreclaim(l);
			}
			/* do outlier detection on sampling results;
			 * this guards against skew */
			if (JOIN_EQ == JOIN_EQ) {
				for (_avg = _tot / _slices, _idx = 0; _idx < _slices; _idx++) {
					BUN _diff = _cnt[_idx] - _avg;

					if (_avg > _cnt[_idx])
						_diff = _avg - _cnt[_idx];
					if (_diff > MAX(SAMPLE_SLICE_SIZE, _avg))
						break;
				}
				if (_idx < _slices) {
					/* outliers detected, compute
					 * a real sample on at most 1%
					 * of the data */
					_sample = MIN(_lcount / 100, (1 << SAMPLE_TRESHOLD_LOG) / 3);
					_tmp2 = BATsample(_tmp1, _sample);
					if (_tmp2) {
						_tmp3 = BATjoin(_tmp2, r, BUN_NONE);	/* might be expensive */
						if (_tmp3) {
							_tot = BATcount(_tmp3);
							BBPreclaim(_tmp3);
						}
						BBPreclaim(_tmp2);
					}
					if (_tmp3 == NULL) {
						GDKfree(_cnt);
						return NULL;
					}
				}
			}
			GDKfree(_cnt);
			/* overestimate always by 5% */
			{
				double _d = (double) (((lng) _tot) * ((lng) _lcount)) / (0.95 * (double) _sample);
				if (_d < (double) BUN_MAX)
					 _estimate = (BUN) _d;
				else
					 _estimate = BUN_MAX;
			}
			l = _tmp1;
		} else {
			BUN _m = MIN(_lcount,_rcount);
			if (_m <= BUN_MAX / 32)
				_m *= 32;
			else
				_m = BUN_MAX;
			 _estimate = MIN(_m,MAX(_lcount,_rcount));
		}
	}


#line 173 "gdk_relop.mx"

		bn = BATnew(BAThtype(l), BATttype(r), _estimate);
		if (bn == NULL)
			return bn;
	}


#line 322 "gdk_relop.mx"

	}

	/* the algorithm */
	loc = ATOMstorage(l->ttype);

	l_last = BUNlast(l);
	r_last = BUNlast(r);
	l_start = l_end = BUNfirst(l);
	r_start = r_end = BUNfirst(r);

	switch (loc) {
	case TYPE_bte:
		
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtloc(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtloc(li, l_end);
				} while (simple_EQ(v1, v2, bte));

			/* lookup value in r (if not nil, that is) */
			if (!simple_EQ(v1, nil, bte)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = simple_CMP(v1, v2, bte);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !simple_EQ(v1, v2, bte);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!simple_EQ(v1, v2, bte))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 335 "gdk_relop.mx"

		break;
	case TYPE_sht:
		
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtloc(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtloc(li, l_end);
				} while (simple_EQ(v1, v2, sht));

			/* lookup value in r (if not nil, that is) */
			if (!simple_EQ(v1, nil, sht)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = simple_CMP(v1, v2, sht);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !simple_EQ(v1, v2, sht);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!simple_EQ(v1, v2, sht))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 338 "gdk_relop.mx"

		break;
	case TYPE_int:
		
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtloc(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtloc(li, l_end);
				} while (simple_EQ(v1, v2, int));

			/* lookup value in r (if not nil, that is) */
			if (!simple_EQ(v1, nil, int)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = simple_CMP(v1, v2, int);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !simple_EQ(v1, v2, int);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!simple_EQ(v1, v2, int))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 341 "gdk_relop.mx"

		break;
	case TYPE_flt:
		
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtloc(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtloc(li, l_end);
				} while (simple_EQ(v1, v2, flt));

			/* lookup value in r (if not nil, that is) */
			if (!simple_EQ(v1, nil, flt)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = simple_CMP(v1, v2, flt);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !simple_EQ(v1, v2, flt);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!simple_EQ(v1, v2, flt))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 344 "gdk_relop.mx"

		break;
	case TYPE_lng:
		
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtloc(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtloc(li, l_end);
				} while (simple_EQ(v1, v2, lng));

			/* lookup value in r (if not nil, that is) */
			if (!simple_EQ(v1, nil, lng)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = simple_CMP(v1, v2, lng);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !simple_EQ(v1, v2, lng);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!simple_EQ(v1, v2, lng))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 347 "gdk_relop.mx"

		break;
	case TYPE_dbl:
		
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtloc(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtloc(li, l_end);
				} while (simple_EQ(v1, v2, dbl));

			/* lookup value in r (if not nil, that is) */
			if (!simple_EQ(v1, nil, dbl)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = simple_CMP(v1, v2, dbl);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !simple_EQ(v1, v2, dbl);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!simple_EQ(v1, v2, dbl))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 350 "gdk_relop.mx"

		break;
	default:
		/* watch it: l->tvarsized may be set due to void l */
		if (l->tvarsized) {
			var = ATOMstorage(l->ttype);

			if (r->hvarsized) {
				/* l and r both real varsized types */
				
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtvar(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtvar(li, l_end);
				} while (atom_EQ(v1, v2, var));

			/* lookup value in r (if not nil, that is) */
			if (!atom_EQ(v1, nil, var)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhvar(ri, r_end);
						neq = atom_CMP(v1, v2, var);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhvar(ri, r_start);
						neq = !atom_EQ(v1, v2, var);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhvar(ri, r_end);
						if (!atom_EQ(v1, v2, var))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 359 "gdk_relop.mx"

			} else {
				/* l is void, r is oid */
				loc = ATOMstorage(r->htype);
				
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtvar(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtvar(li, l_end);
				} while (atom_EQ(v1, v2, loc));

			/* lookup value in r (if not nil, that is) */
			if (!atom_EQ(v1, nil, loc)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = atom_CMP(v1, v2, loc);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !atom_EQ(v1, v2, loc);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!atom_EQ(v1, v2, loc))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 363 "gdk_relop.mx"

			}
		} else {
			/* we can't handle void r anyway, so don't
			 * worry about it here */
			loc = ATOMstorage(l->ttype);
			
#line 210 "gdk_relop.mx"
	if (((!BATtvoid(l)) || l->tseqbase != oid_nil) &&
	    ((!BAThvoid(r)) || r->hseqbase != oid_nil || nil_on_miss)) {
		assert(r->htype != TYPE_void);
		while (l_start < l_last) {
			ptr v2, v1 = BUNtloc(li, l_start);
			int neq = 1;

			/* lookup range in l */
			l_end = l_start;
			if (l_key) {
				l_end++;
			} else
				do {
					if ((++l_end) >= l_last)
						break;
					v2 = BUNtloc(li, l_end);
				} while (atom_EQ(v1, v2, loc));

			/* lookup value in r (if not nil, that is) */
			if (!atom_EQ(v1, nil, loc)) {
				if (r_scan > 0) {
					/* first try scanning; but
					 * give up after a while */
					for (r_lim = MIN(r_last, r_end + r_scan); r_end < r_lim; r_end++) {
						v2 = BUNhloc(ri, r_end);
						neq = atom_CMP(v1, v2, loc);
						if (neq <= 0)
							break;
					}
					r_start = r_end;
				}
				if (neq == 1) {
					/* use binary search after
					 * failed scan or if scanning
					 * is impossible (l not
					 * sorted) */
					if (r_scan < 0 || r_start < r_last) {
						/* if merge not ended
						 * (or if no merge at
						 * all) */
						r_start = SORTfndfirst(rr, v1);
					}
					if (r_start < r_last) {
						v2 = BUNhloc(ri, r_start);
						neq = !atom_EQ(v1, v2, loc);
					} else if (r_scan >= 0) {
						/* r is already at end
						 * => break off merge
						 * join */
						break;
					}
				}
			}
			if (neq == 0) {
				/* lookup range in r */
				r_end = r_start+1;
				if (r_key == 0)
					while (r_end < r_last) {
						v2 = BUNhloc(ri, r_end);
						if (!atom_EQ(v1, v2, loc))
							break;
						r_end++ ;
					}
				/* generate match-product as join result */
				for (; l_start < l_end; l_start++)
					for (r_cur = r_start; r_cur < r_end; r_cur++)
						bunfastins_limit(bn, BUNhead(li, l_start), BUNtail(ri, r_cur), limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),r_start,r_cur,r_end));
			} else if (nil_on_miss) {
				/* outerjoin inserts nils on a miss */
				hasnils = 1;
				for (; l_start < l_end; l_start++)
					bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l),l_start,BUNlast(l),0,1,1));
			} else {
				l_start = l_end;	/* no match found in equi-join */
			}
		}
	}


#line 369 "gdk_relop.mx"

		}
		break;
	}

	if (nil_on_miss && l_start < l_last) {
		hasnils = 1;
		for (; l_start < l_last; l_start++)
			bunfastins_limit(bn, BUNhead(li, l_start), nil_on_miss, limit, BATPERC(BUNfirst(l), l_start, BUNlast(l), 0, 1, 1));
	}
	/* propagate properties */
      bunins_done:
	bn->hsorted = BAThordered(l);
	bn->hrevsorted = BAThrevordered(l);
	if (r->hkey) {
		if (BATcount(bn) == BATcount(l)) {
			ALIGNsetH(bn, l);
		} else if (l->hkey) {
			BATkey(bn, TRUE);
		}
	}
	bn->H->nonil = l->H->nonil;
	bn->tsorted = FALSE;
	bn->trevsorted = FALSE;
	if (!hasnils) {
		if (BATtordered(l)) {
			if (l->tkey && BATcount(bn) == BATcount(r)) {
				ALIGNsetT(bn, r);
			} else if (l->tkey || r->hkey) {
				bn->tsorted = BATtordered(r);
				bn->trevsorted = BATtrevordered(r);
			}
		}
		if (l->tkey && r->tkey) {
			BATkey(BATmirror(bn), TRUE);
		}
	}
	bn->T->nonil = r->T->nonil && !hasnils;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

static BAT *batfetchjoin(BAT *l, BAT *r, BUN estimate, bit swap, bit hitalways);

static BAT *
batmergejoin(BAT *l, BAT *r, BUN estimate, bit swap, BUN *limit)
{
	
#line 60 "gdk_relop.mx"
	ERRORcheck(l == NULL, "BATmergejoin: invalid left operand");
	ERRORcheck(r == NULL, "BATmergejoin: invalid right operand");
	ERRORcheck(TYPEerror(l->ttype, r->htype), "BATmergejoin: type conflict\n");


#line 418 "gdk_relop.mx"

	if (BAThdense(r) || (swap && BATtdense(l))) {
		/* batmergejoin can't handle void tail columns at all
		 * (fetchjoin is better anyway) */
		BAT *left = limit ? BATslice(l, 0, *limit) : l;
		BAT *bn = batfetchjoin(left, r, estimate, swap, FALSE);
		if (limit)
			BBPreclaim(left);
		return bn;
	}
	if (swap && (!BAThordered(r) || (BATtordered(l) && BATcount(l) > BATcount(r)))) {
		/* reverse join if required (r not sorted) or if l is
		 * larger (quick jump through l with binary search) */
		BAT *bn = mergejoin(BATmirror(r), BATmirror(l), NULL, NULL, estimate, limit);

		return bn ? BATmirror(bn) : NULL;
	}
	return mergejoin(l, r, NULL, NULL, estimate, limit);
}

BAT *
BATmergejoin(BAT *l, BAT *r, BUN estimate)
{
	/* allows swapping of left and right input for faster processing */
	return batmergejoin(l, r, estimate, TRUE, NULL);
}

/*
 * @- hash join
 * These macros encode the core of the join algorithm. They are
 * the fast inner loops, optimized towards their type.
 */


#line 472 "gdk_relop.mx"
BAT *
BAThashjoin(BAT *l, BAT *r, BUN estimate)
{
	ptr v, nil = ATOMnilptr(r->htype);
	BUN p, q;
	int any;
	BAT *bn = NULL;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	
#line 60 "gdk_relop.mx"
	ERRORcheck(l == NULL, "BAThashjoin: invalid left operand");
	ERRORcheck(r == NULL, "BAThashjoin: invalid right operand");
	ERRORcheck(TYPEerror(l->ttype, r->htype), "BAThashjoin: type conflict\n");


#line 482 "gdk_relop.mx"

	
#line 170 "gdk_relop.mx"
	{
		BUN _estimate = estimate;

		
#line 64 "gdk_relop.mx"
	if ( _estimate == BUN_NONE) {
		BUN _lcount = BATcount(l);
		BUN _rcount = BATcount(r);
		BUN _slices = 0;

		/* limit estimate with simple bounds first; only spend
		 * effort if the join result might be big */
		if (JOIN_EQ == JOIN_EQ) {
			if (l->tkey)
				 _estimate = r->hkey ? MIN(_rcount, _lcount) : _rcount;
			else if (r->hkey)
				 _estimate = _lcount;
		}
		if ( _estimate == BUN_NONE) {
			BUN _heuristic = MIN(_lcount, _rcount);

			if (_heuristic <= BUN_MAX / 3) {
				_heuristic *= 3;
				if (_heuristic <= (1 << SAMPLE_TRESHOLD_LOG))
					 _estimate = _heuristic;
			}
		}
		if ( _estimate == BUN_NONE) {
			BUN _idx;

			for (_idx = _lcount; _idx > 0; _idx >>= 1)
				_slices++;
		}
		if (_slices > SAMPLE_TRESHOLD_LOG) {
			/* use cheapo sampling by taking a number of
			 * slices and joining those with the algo */
			BUN _idx = 0, _tot = 0, _step, _lo, _avg, _sample, *_cnt;
			BAT *_tmp1 = l, *_tmp2, *_tmp3 = NULL;

			_step = _lcount / (_slices -= SAMPLE_TRESHOLD_LOG);
			_sample = _slices * SAMPLE_SLICE_SIZE;
			_cnt = GDKmalloc(_slices * sizeof(BUN));
			if (_cnt == NULL)
				return NULL;
			for (_lo = 0; _idx < _slices; _lo += _step) {
				BUN _size = 0, _hi = _lo + SAMPLE_SLICE_SIZE;

				l = BATslice(_tmp1, _lo, _hi);	/* slice keeps all parent properties */
				if (l == NULL) {
					GDKfree(_cnt);
					return NULL;
				}
				_tmp2 =  BAThashjoin(l,r,BUN_NONE);	/*  BAThashjoin(l,r,BUN_NONE) = e.g. BATXjoin(l,r) */
				if (_tmp2) {
					_size = BATcount(_tmp2);
					BBPreclaim(_tmp2);
				}
				_tot += (_cnt[_idx++] = _size);
				BBPreclaim(l);
			}
			/* do outlier detection on sampling results;
			 * this guards against skew */
			if (JOIN_EQ == JOIN_EQ) {
				for (_avg = _tot / _slices, _idx = 0; _idx < _slices; _idx++) {
					BUN _diff = _cnt[_idx] - _avg;

					if (_avg > _cnt[_idx])
						_diff = _avg - _cnt[_idx];
					if (_diff > MAX(SAMPLE_SLICE_SIZE, _avg))
						break;
				}
				if (_idx < _slices) {
					/* outliers detected, compute
					 * a real sample on at most 1%
					 * of the data */
					_sample = MIN(_lcount / 100, (1 << SAMPLE_TRESHOLD_LOG) / 3);
					_tmp2 = BATsample(_tmp1, _sample);
					if (_tmp2) {
						_tmp3 = BATjoin(_tmp2, r, BUN_NONE);	/* might be expensive */
						if (_tmp3) {
							_tot = BATcount(_tmp3);
							BBPreclaim(_tmp3);
						}
						BBPreclaim(_tmp2);
					}
					if (_tmp3 == NULL) {
						GDKfree(_cnt);
						return NULL;
					}
				}
			}
			GDKfree(_cnt);
			/* overestimate always by 5% */
			{
				double _d = (double) (((lng) _tot) * ((lng) _lcount)) / (0.95 * (double) _sample);
				if (_d < (double) BUN_MAX)
					 _estimate = (BUN) _d;
				else
					 _estimate = BUN_MAX;
			}
			l = _tmp1;
		} else {
			BUN _m = MIN(_lcount,_rcount);
			if (_m <= BUN_MAX / 32)
				_m *= 32;
			else
				_m = BUN_MAX;
			 _estimate = MIN(_m,MAX(_lcount,_rcount));
		}
	}


#line 173 "gdk_relop.mx"

		bn = BATnew(BAThtype(l), BATttype(r), _estimate);
		if (bn == NULL)
			return bn;
	}


#line 483 "gdk_relop.mx"


	if (BATprepareHash(r)) {
		return NULL;
	}
	switch (any = ATOMstorage(l->ttype)) {
	case TYPE_bte:
		
#line 451 "gdk_relop.mx"
	{
		BUN yy;

		BATloop(l, p, q) {
			v = BUNtloc(li, p);
			if (simple_EQ(v, nil, bte)) {
				continue; /* skip nil */
			}
			HASHloop_bte(ri, r->H->hash, yy, v) {
				bunfastins(bn, BUNhead(li, p), BUNtail(ri, yy));
			}
		}
		/* set sorted flags by hand, because we used BUNfastins() */
		bn->hsorted = BAThordered(l);
		bn->hrevsorted = BAThrevordered(l);
		bn->tsorted = FALSE;
		bn->trevsorted = FALSE;
		break;
	}


#line 490 "gdk_relop.mx"

	case TYPE_sht:
		
#line 451 "gdk_relop.mx"
	{
		BUN yy;

		BATloop(l, p, q) {
			v = BUNtloc(li, p);
			if (simple_EQ(v, nil, sht)) {
				continue; /* skip nil */
			}
			HASHloop_sht(ri, r->H->hash, yy, v) {
				bunfastins(bn, BUNhead(li, p), BUNtail(ri, yy));
			}
		}
		/* set sorted flags by hand, because we used BUNfastins() */
		bn->hsorted = BAThordered(l);
		bn->hrevsorted = BAThrevordered(l);
		bn->tsorted = FALSE;
		bn->trevsorted = FALSE;
		break;
	}


#line 492 "gdk_relop.mx"

	case TYPE_int:
	case TYPE_flt:
		
#line 451 "gdk_relop.mx"
	{
		BUN yy;

		BATloop(l, p, q) {
			v = BUNtloc(li, p);
			if (simple_EQ(v, nil, int)) {
				continue; /* skip nil */
			}
			HASHloop_int(ri, r->H->hash, yy, v) {
				bunfastins(bn, BUNhead(li, p), BUNtail(ri, yy));
			}
		}
		/* set sorted flags by hand, because we used BUNfastins() */
		bn->hsorted = BAThordered(l);
		bn->hrevsorted = BAThrevordered(l);
		bn->tsorted = FALSE;
		bn->trevsorted = FALSE;
		break;
	}


#line 495 "gdk_relop.mx"

	case TYPE_dbl:
	case TYPE_lng:
		
#line 451 "gdk_relop.mx"
	{
		BUN yy;

		BATloop(l, p, q) {
			v = BUNtloc(li, p);
			if (simple_EQ(v, nil, lng)) {
				continue; /* skip nil */
			}
			HASHloop_lng(ri, r->H->hash, yy, v) {
				bunfastins(bn, BUNhead(li, p), BUNtail(ri, yy));
			}
		}
		/* set sorted flags by hand, because we used BUNfastins() */
		bn->hsorted = BAThordered(l);
		bn->hrevsorted = BAThrevordered(l);
		bn->tsorted = FALSE;
		bn->trevsorted = FALSE;
		break;
	}


#line 498 "gdk_relop.mx"

	case TYPE_str:
		if (l->T->vheap->hashash) {
			
#line 451 "gdk_relop.mx"
	{
		BUN yy;

		BATloop(l, p, q) {
			v = BUNtail(li, p);
			if (atom_EQ(v, nil, any)) {
				continue; /* skip nil */
			}
			HASHloop_str_hv(ri, r->H->hash, yy, v) {
				bunfastins(bn, BUNhead(li, p), BUNtail(ri, yy));
			}
		}
		/* set sorted flags by hand, because we used BUNfastins() */
		bn->hsorted = BAThordered(l);
		bn->hrevsorted = BAThrevordered(l);
		bn->tsorted = FALSE;
		bn->trevsorted = FALSE;
		break;
	}


#line 501 "gdk_relop.mx"

		}
		/* fall through */
	default:
		
#line 451 "gdk_relop.mx"
	{
		BUN yy;

		BATloop(l, p, q) {
			v = BUNtail(li, p);
			if (atom_EQ(v, nil, any)) {
				continue; /* skip nil */
			}
			HASHloop_any(ri, r->H->hash, yy, v) {
				bunfastins(bn, BUNhead(li, p), BUNtail(ri, yy));
			}
		}
		/* set sorted flags by hand, because we used BUNfastins() */
		bn->hsorted = BAThordered(l);
		bn->hrevsorted = BAThrevordered(l);
		bn->tsorted = FALSE;
		bn->trevsorted = FALSE;
		break;
	}


#line 505 "gdk_relop.mx"

	}

	/* propagate alignment info */
	bn->hsorted = BAThordered(l);
	bn->hrevsorted = BAThrevordered(l);
	if (BAThkey(r)) {
		if (BATcount(bn) == BATcount(l))
			ALIGNsetH(bn, l);
		if (BAThkey(l))
			BATkey(bn, TRUE);
	}
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = r->T->nonil;
	ESTIDEBUG THRprintf(GDKout, "#BAThashjoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;

}


/*
 * @- Fetch-join
 * The BATfetchjoin(l,r) does a join on the basis of positional lookup.
 * It looks up index numbers from the second parameter in first parameter BAT.
 * The right parameter may contain OIDs, in which case their base is
 * subtracted.
 *
 * In a typical join(BAT[any::1,oid) L, BATvoid,any::2] R) : BAT[any::1,any::2]
 * we expect each tuple of L to hit exactly once in R. Now if any::1=void
 * this void column can be carried over to the result. We do that.
 *
 * However, it is possible that an tail-oid is out of range with respect
 * to R; in that case some tuples will be missing and we cannot carry on
 * producing a void column. In that case, we have to switch back
 * on-the-fly to the non-dense implementation.
 *
 * The aftermath -- property setting -- is relatively straightforward here.
 */
#define HLATOMput(bn, dst) ATOMput(bn->htype, bn->H->vheap, dst, BUNhloc(li,l_cur))
#define HVATOMput(bn, dst) Hputvalue(bn, dst, BUNhvar(li,l_cur), 1)
#define TLATOMput(bn, dst) ATOMput(bn->ttype, bn->T->vheap, dst, BUNtloc(ri,r_cur))
#define TVATOMput(bn, dst) Tputvalue(bn, dst, BUNtvar(ri,r_cur), 1)
#define LATOM_cmp(bn, p,n) atom_CMP(p, n, bn->ttype)
#define VATOM_cmp(bn, p,n) atom_CMP(p, n, bn->ttype)



#line 560 "gdk_relop.mx"

#line 555 "gdk_relop.mx"
#define Hbteput(bn,dst)	*(bte*) (dst) = *(bte*) (BUNhloc(li,l_cur))
#define Tbteput(bn,dst)	*(bte*) (dst) = *(bte*) (BUNtloc(ri,r_cur))
#define bte_cmp(bn,p,n)  simple_CMP(p, n, bte)


#line 560 "gdk_relop.mx"


#line 555 "gdk_relop.mx"
#define Hshtput(bn,dst)	*(sht*) (dst) = *(sht*) (BUNhloc(li,l_cur))
#define Tshtput(bn,dst)	*(sht*) (dst) = *(sht*) (BUNtloc(ri,r_cur))
#define sht_cmp(bn,p,n)  simple_CMP(p, n, sht)


#line 561 "gdk_relop.mx"


#line 555 "gdk_relop.mx"
#define Hintput(bn,dst)	*(int*) (dst) = *(int*) (BUNhloc(li,l_cur))
#define Tintput(bn,dst)	*(int*) (dst) = *(int*) (BUNtloc(ri,r_cur))
#define int_cmp(bn,p,n)  simple_CMP(p, n, int)


#line 562 "gdk_relop.mx"


#line 555 "gdk_relop.mx"
#define Hfltput(bn,dst)	*(flt*) (dst) = *(flt*) (BUNhloc(li,l_cur))
#define Tfltput(bn,dst)	*(flt*) (dst) = *(flt*) (BUNtloc(ri,r_cur))
#define flt_cmp(bn,p,n)  simple_CMP(p, n, flt)


#line 563 "gdk_relop.mx"


#line 555 "gdk_relop.mx"
#define Hlngput(bn,dst)	*(lng*) (dst) = *(lng*) (BUNhloc(li,l_cur))
#define Tlngput(bn,dst)	*(lng*) (dst) = *(lng*) (BUNtloc(ri,r_cur))
#define lng_cmp(bn,p,n)  simple_CMP(p, n, lng)


#line 564 "gdk_relop.mx"


#line 555 "gdk_relop.mx"
#define Hdblput(bn,dst)	*(dbl*) (dst) = *(dbl*) (BUNhloc(li,l_cur))
#define Tdblput(bn,dst)	*(dbl*) (dst) = *(dbl*) (BUNtloc(ri,r_cur))
#define dbl_cmp(bn,p,n)  simple_CMP(p, n, dbl)


#line 565 "gdk_relop.mx"




#line 572 "gdk_relop.mx"


#line 713 "gdk_relop.mx"


#line 724 "gdk_relop.mx"

#line 714 "gdk_relop.mx"
	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 714 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 715 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 716 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 717 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 718 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 719 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 720 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_bte_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(bte,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_bte_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(bte,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_bte_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(bte,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 721 "gdk_relop.mx"



#line 724 "gdk_relop.mx"


#line 714 "gdk_relop.mx"
	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 714 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 715 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 716 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 717 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 718 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 719 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 720 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_sht_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(sht,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_sht_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(sht,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_sht_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(sht,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 721 "gdk_relop.mx"



#line 725 "gdk_relop.mx"


#line 714 "gdk_relop.mx"
	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 714 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 715 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 716 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 717 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 718 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 719 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 720 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_int_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(int,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_int_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(int,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_int_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(int,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 721 "gdk_relop.mx"



#line 726 "gdk_relop.mx"


#line 714 "gdk_relop.mx"
	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 714 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 715 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 716 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 717 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 718 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 719 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 720 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_lng_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(lng,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_lng_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(lng,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_lng_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(lng,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 721 "gdk_relop.mx"



#line 727 "gdk_relop.mx"



#line 714 "gdk_relop.mx"
	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 714 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 715 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 716 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 717 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 718 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 719 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 720 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_VATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(VATOM,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_VATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(VATOM,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_VATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(VATOM,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 721 "gdk_relop.mx"



#line 729 "gdk_relop.mx"


#line 714 "gdk_relop.mx"
	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,bte,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 714 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,sht,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 715 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,int,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 716 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_flt(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,flt,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tfltput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 717 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,lng,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 718 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_dbl(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,dbl,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tdblput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 719 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,VATOM,var);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 720 "gdk_relop.mx"

	
#line 573 "gdk_relop.mx"
static BAT *
densefetchjoin_LATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	r_cur = (BUN) (offset + *(oid *) BUNtail(li, BUNfirst(l)));

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densefetchjoin(LATOM,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 595 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
orderedfetchjoin_LATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN base, yy;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedfetchjoin(LATOM,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 641 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}

static BAT *
defaultfetchjoin_LATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BUN l_cur, l_end, r_cur, dst;
	ssize_t offset;
	BUN yy, base, end;
	BAT *ret = NULL;
	BATiter bni = bat_iterator(bn);
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	void *prev = NULL, *next = NULL;

	if (bn == NULL)
		return NULL;
	dst = BUNfirst(bn);
	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase; /* cast first, subtract second */
	end = base + BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultfetchjoin(LATOM,LATOM,loc);\n");

	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNtail(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 690 "gdk_relop.mx"

		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	BATsetcount(bn, dst);
	ret = bn;
      goto bunins_failed;
      bunins_failed:
	if (!ret)
		BBPreclaim(bn);
	return ret;
}


#line 721 "gdk_relop.mx"



#line 730 "gdk_relop.mx"




#line 752 "gdk_relop.mx"


#line 770 "gdk_relop.mx"


#line 790 "gdk_relop.mx"


#line 811 "gdk_relop.mx"


#line 873 "gdk_relop.mx"


#line 909 "gdk_relop.mx"
#define SIMPLEput(tpe,hp,dst,src) *(tpe*) (dst) = *(tpe*) (src)

static BAT *
batfetchjoin(BAT *l, BAT *r, BUN estimate, bit swap, bit hitalways)
{
	int lht, rtt;
	BUN base, end;
	ssize_t offset;
	BUN lcount, rcount;
	BUN r_cur, l_cur, l_end;
	oid seqbase;
	BAT *ret = NULL, *bn = NULL, *l_orig = l;
	bit hitalways_check = FALSE;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	bit derive_tail_properties = FALSE;

	BATcheck(l, "BATfetchjoin: left BAT required");
	BATcheck(r, "BATfetchjoin: right BAT required");

	lcount = BATcount(l);
	rcount = BATcount(r);
	if (estimate == BUN_NONE || estimate < lcount) {
		/* upper bound to avoid size checks in the join loop */
		estimate = lcount;
	}

	if (swap) {
		if (!BAThdense(r)) {
			ERRORcheck(!BATtdense(l), "BATfetchjoin: one join column must be dense");
			ALGODEBUG fprintf(stderr, "#BATfetchjoin: BATmirror(BATfetchjoin(BATmirror(r),BATmirror(l)));\n");

			return BATmirror(batfetchjoin(BATmirror(r), BATmirror(l), estimate, FALSE, FALSE));
		}
	} else {
		ERRORcheck(!BAThdense(r), "BATfetchjoin: head column of right input must be dense");
	}
	/* not checking boundaries is very dangerous; use regression
	 * tests with debugmask=8 first */
	PROPDEBUG {
		hitalways_check = hitalways;
		hitalways = FALSE;
	}

	if (lcount == 0 || rcount == 0) {
		/* below range checking do not support empty bats. so
		 * treat them separately (easy) */
		
#line 958 "gdk_relop.mx"
		ALGODEBUG fprintf(stderr, "# BATfetchjoin: |l|==0 or |r|==0 => empty result\n");
#if 1
		if (hitalways|hitalways_check && lcount > 0) {
			GDKerror("BATfetchjoin(%s,%s) does not hit always (|bn|=0 != "BUNFMT"=|l|) => can't use fetchjoin.\n", BATgetId(l), BATgetId(r), lcount);
			return NULL;
		}
#endif
		bn = BATnew(l_orig->htype, r->ttype, 0);
		if (bn == NULL)
			return NULL;
		BATkey(bn, TRUE);
		BATkey(BATmirror(bn), TRUE);
		if (bn->htype == TYPE_void || bn->htype == TYPE_oid) {
			BATseqbase(bn, l_orig->htype == TYPE_void ? l_orig->hseqbase : 0);
			bn->hdense = bn->hseqbase != oid_nil;
		}
		if (bn->ttype == TYPE_void || bn->ttype == TYPE_oid) {
			BATseqbase(BATmirror(bn), r->ttype == TYPE_void ? r->tseqbase : 0);
			bn->tdense = bn->tseqbase != oid_nil;
		}
		return bn;


#line 956 "gdk_relop.mx"



#line 981 "gdk_relop.mx"
	} else if (BATtdense(l) && BAThdense(r) && l->tseqbase == r->hseqbase && BATcount(l) <= BATcount(r)) {
		/* always hit and tail of left is alligned with head
		 * of right */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BATtdense(l) && BAThdense(r)\n");
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: VIEWcreate(l,r)\n");

		return VIEWcreate(l, r);
	} else if (hitalways && BAThdense(r) && BATtdense(r) && r->hseqbase == r->tseqbase && BATcount(l) <= BATcount(r)) {
		/* idempotent join: always hit and substitute tail with the same value */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThdense(r) && BATtdense(r)\n");
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: VIEWcreate(l,l)\n");

		return VIEWcreate(l, l);
	} else if (BATtordered(l)) {
		/* optimization to be able to carry over more void
		 * head columns */
		/* (only needed if neither operand is empty) */
		oid r_lo = *(oid *) BUNhead(ri, BUNfirst(r));
		oid r_hi = *(oid *) BUNhead(ri, BUNlast(r) - 1);
		oid l_lo = *(oid *) BUNtail(li, BUNfirst(l));
		oid l_hi = *(oid *) BUNtail(li, BUNlast(l) - 1);
		int empty = r_lo > l_hi || r_hi < l_lo;
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BATtordered(l)\n");
		ALGODEBUG fprintf(stderr, "#r_lo=" OIDFMT ", r_hi=" OIDFMT ", l_lo=" OIDFMT ", l_hi=" OIDFMT ".\n", r_lo, r_hi, l_lo, l_hi);

		if (!empty && (r_lo > l_lo || r_hi < l_hi)) {
			ALGODEBUG fprintf(stderr, "#shrinking!\n");
			ALGODEBUG fprintf(stderr, "#BATfetchjoin: l = BATselect(l, &r_lo, &r_hi);\n");

			li.b = l = BATselect(l, &r_lo, &r_hi);	/* sorted, so it will be a slice */
			if (l == NULL)
				return NULL;
			if (BATcount(l) == 0) {
				if (l != l_orig) {
					BBPreclaim(l);	/* was created as a temporary (slice) select on l */
				}
				empty = 1;
			}
		}
		if (empty) {
			
#line 958 "gdk_relop.mx"
		ALGODEBUG fprintf(stderr, "# BATfetchjoin: empty => empty result\n");
#if 1
		if (hitalways|hitalways_check && lcount > 0) {
			GDKerror("BATfetchjoin(%s,%s) does not hit always (|bn|=0 != "BUNFMT"=|l|) => can't use fetchjoin.\n", BATgetId(l), BATgetId(r), lcount);
			return NULL;
		}
#endif
		bn = BATnew(l_orig->htype,  r->ttype, 0);
		if (bn == NULL)
			return NULL;
		BATkey(bn, TRUE);
		BATkey(BATmirror(bn), TRUE);
		if (bn->htype == TYPE_void || bn->htype == TYPE_oid) {
			BATseqbase(bn, l_orig->htype == TYPE_void ? l_orig->hseqbase : 0);
			bn->hdense = bn->hseqbase != oid_nil;
		}
		if (bn->ttype == TYPE_void || bn->ttype == TYPE_oid) {
			BATseqbase(BATmirror(bn),  r->ttype == TYPE_void ?  r->tseqbase : 0);
			bn->tdense = bn->tseqbase != oid_nil;
		}
		return bn;


#line 1021 "gdk_relop.mx"

		} else if (hitalways | hitalways_check && BATcount(l) < lcount) {
			GDKerror("BATfetchjoin(%s,%s) does not hit always (|bn|=" BUNFMT " != " BUNFMT "=|l|) => can't use fetchjoin.\n", BATgetId(l), BATgetId(r), BATcount(l), lcount);
			return NULL;
		}
		lcount = BATcount(l);
	}
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: 1\n");

	base = BUNfirst(r);
	offset = (ssize_t) base - (ssize_t) r->hseqbase;	/* cast first, subtract second */
	end = base + rcount;
	/* only BUNhead crashes on empty bats with TYPE != virtual oid */
	seqbase = l->htype == TYPE_void ?
		l->hseqbase :
		(lcount ?
		 (l->htype == TYPE_int ?
		  (oid) *(int *) BUNhead(li, BUNfirst(l)) :
		  (l->htype == TYPE_oid ?
		   *(oid *) BUNhead(li, BUNfirst(l)) :
		   (l->htype == TYPE_lng ?
		    (oid) *(lng *) BUNhead(li, BUNfirst(l)) :
		    oid_nil))) :
		 oid_nil);

	ALGODEBUG fprintf(stderr, "#BATfetchjoin: 2\n");

	if (!BAThvoid(l)) {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: !BAThvoid(l)\n");

		/* default case: no void column to propagate */
		lht = l->htype;
		rtt = r->ttype;

		if (ATOMstorage(rtt) == TYPE_str &&
		    (BATtordered(l) || r->T->vheap->free < MT_MMAP_TILE) &&
		    (!rcount || (lcount << 3) > rcount)) {
			/* insert as ints and copy/share the string heap */
			rtt = r->T->width == 1 ? TYPE_bte : (r->T->width == 2 ? TYPE_sht : (r->T->width == 4 ? TYPE_int : TYPE_lng));
		}
		bn = BATnew(BAThtype(l), ATOMtype(rtt), estimate);
		if (bn == NULL)
			goto ready;
		ESTIDEBUG THRprintf(GDKout, "#BATfetchjoin: estimated resultsize: " BUNFMT "\n", lcount);

		if ((BATtordered(l) & BATtordered(r)) || \
		    (BATtordered(l) & BATtrevordered(r)) || \
		    (BATtrevordered(l) & BATtordered(r)) || \
		    (BATtrevordered(l) & BATtrevordered(r))) {
			/* will be set correctly once we're done */
			bn->tsorted = bn->trevsorted = 0;
		} else if (rtt != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
			/* "string trick" => cannot compare on the fly */
			bn->tsorted = bn->trevsorted = 0;
			derive_tail_properties = TRUE;
		} else if (r->ttype == TYPE_void) {
			/* cannot compare on the fly */
			bn->tsorted = bn->trevsorted = 0;
			derive_tail_properties = TRUE;
		} else {
			/* start optimistic & check on the fly */
			bn->tsorted = bn->trevsorted = 1;
		}
		/* TODO: apply the "string trick" (see below) here too */
		if (BATtdense(l)) {
			/* dense => ordered, i.e., we did check the
			 * bounderies already above and we can do a
			 * "synchronized walk" through l & r */
			ALGODEBUG fprintf(stderr, "#BATfetchjoin: !BAThvoid(l) && BATtdense(l)\n");
			
#line 753 "gdk_relop.mx"
	if (ATOMstorage(lht) == TYPE_bte) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = densefetchjoin_bte_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = densefetchjoin_bte_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = densefetchjoin_bte_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = densefetchjoin_bte_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = densefetchjoin_bte_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = densefetchjoin_bte_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = densefetchjoin_bte_VATOM(bn, l, r);
	} else {
		bn = densefetchjoin_bte_LATOM(bn, l, r);
	}


#line 754 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_sht) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = densefetchjoin_sht_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = densefetchjoin_sht_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = densefetchjoin_sht_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = densefetchjoin_sht_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = densefetchjoin_sht_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = densefetchjoin_sht_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = densefetchjoin_sht_VATOM(bn, l, r);
	} else {
		bn = densefetchjoin_sht_LATOM(bn, l, r);
	}


#line 756 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_int ||
		   ATOMstorage(lht) == TYPE_flt) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = densefetchjoin_int_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = densefetchjoin_int_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = densefetchjoin_int_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = densefetchjoin_int_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = densefetchjoin_int_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = densefetchjoin_int_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = densefetchjoin_int_VATOM(bn, l, r);
	} else {
		bn = densefetchjoin_int_LATOM(bn, l, r);
	}


#line 759 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_lng ||
		   ATOMstorage(lht) == TYPE_dbl) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = densefetchjoin_lng_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = densefetchjoin_lng_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = densefetchjoin_lng_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = densefetchjoin_lng_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = densefetchjoin_lng_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = densefetchjoin_lng_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = densefetchjoin_lng_VATOM(bn, l, r);
	} else {
		bn = densefetchjoin_lng_LATOM(bn, l, r);
	}


#line 762 "gdk_relop.mx"

	} else if (l->hvarsized) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = densefetchjoin_VATOM_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = densefetchjoin_VATOM_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = densefetchjoin_VATOM_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = densefetchjoin_VATOM_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = densefetchjoin_VATOM_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = densefetchjoin_VATOM_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = densefetchjoin_VATOM_VATOM(bn, l, r);
	} else {
		bn = densefetchjoin_VATOM_LATOM(bn, l, r);
	}


#line 764 "gdk_relop.mx"

	} else {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = densefetchjoin_LATOM_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = densefetchjoin_LATOM_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = densefetchjoin_LATOM_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = densefetchjoin_LATOM_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = densefetchjoin_LATOM_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = densefetchjoin_LATOM_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = densefetchjoin_LATOM_VATOM(bn, l, r);
	} else {
		bn = densefetchjoin_LATOM_LATOM(bn, l, r);
	}


#line 766 "gdk_relop.mx"

	}


#line 1090 "gdk_relop.mx"

		} else if (BATtordered(l) || hitalways) {
			/* we did check the bounderies already above
			 * BATtordered(l) or simply "trust"
			 * hitalways */
			ALGODEBUG fprintf(stderr, "#BATfetchjoin: !BAThvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int) hitalways);
			
#line 753 "gdk_relop.mx"
	if (ATOMstorage(lht) == TYPE_bte) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = orderedfetchjoin_bte_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = orderedfetchjoin_bte_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = orderedfetchjoin_bte_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = orderedfetchjoin_bte_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = orderedfetchjoin_bte_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = orderedfetchjoin_bte_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = orderedfetchjoin_bte_VATOM(bn, l, r);
	} else {
		bn = orderedfetchjoin_bte_LATOM(bn, l, r);
	}


#line 754 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_sht) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = orderedfetchjoin_sht_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = orderedfetchjoin_sht_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = orderedfetchjoin_sht_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = orderedfetchjoin_sht_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = orderedfetchjoin_sht_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = orderedfetchjoin_sht_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = orderedfetchjoin_sht_VATOM(bn, l, r);
	} else {
		bn = orderedfetchjoin_sht_LATOM(bn, l, r);
	}


#line 756 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_int ||
		   ATOMstorage(lht) == TYPE_flt) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = orderedfetchjoin_int_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = orderedfetchjoin_int_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = orderedfetchjoin_int_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = orderedfetchjoin_int_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = orderedfetchjoin_int_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = orderedfetchjoin_int_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = orderedfetchjoin_int_VATOM(bn, l, r);
	} else {
		bn = orderedfetchjoin_int_LATOM(bn, l, r);
	}


#line 759 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_lng ||
		   ATOMstorage(lht) == TYPE_dbl) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = orderedfetchjoin_lng_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = orderedfetchjoin_lng_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = orderedfetchjoin_lng_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = orderedfetchjoin_lng_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = orderedfetchjoin_lng_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = orderedfetchjoin_lng_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = orderedfetchjoin_lng_VATOM(bn, l, r);
	} else {
		bn = orderedfetchjoin_lng_LATOM(bn, l, r);
	}


#line 762 "gdk_relop.mx"

	} else if (l->hvarsized) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = orderedfetchjoin_VATOM_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = orderedfetchjoin_VATOM_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = orderedfetchjoin_VATOM_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = orderedfetchjoin_VATOM_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = orderedfetchjoin_VATOM_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = orderedfetchjoin_VATOM_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = orderedfetchjoin_VATOM_VATOM(bn, l, r);
	} else {
		bn = orderedfetchjoin_VATOM_LATOM(bn, l, r);
	}


#line 764 "gdk_relop.mx"

	} else {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = orderedfetchjoin_LATOM_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = orderedfetchjoin_LATOM_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = orderedfetchjoin_LATOM_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = orderedfetchjoin_LATOM_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = orderedfetchjoin_LATOM_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = orderedfetchjoin_LATOM_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = orderedfetchjoin_LATOM_VATOM(bn, l, r);
	} else {
		bn = orderedfetchjoin_LATOM_LATOM(bn, l, r);
	}


#line 766 "gdk_relop.mx"

	}


#line 1096 "gdk_relop.mx"

		} else {
			ALGODEBUG fprintf(stderr, "#BATfetchjoin: !BAThvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
			
#line 753 "gdk_relop.mx"
	if (ATOMstorage(lht) == TYPE_bte) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = defaultfetchjoin_bte_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = defaultfetchjoin_bte_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = defaultfetchjoin_bte_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = defaultfetchjoin_bte_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = defaultfetchjoin_bte_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = defaultfetchjoin_bte_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = defaultfetchjoin_bte_VATOM(bn, l, r);
	} else {
		bn = defaultfetchjoin_bte_LATOM(bn, l, r);
	}


#line 754 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_sht) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = defaultfetchjoin_sht_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = defaultfetchjoin_sht_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = defaultfetchjoin_sht_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = defaultfetchjoin_sht_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = defaultfetchjoin_sht_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = defaultfetchjoin_sht_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = defaultfetchjoin_sht_VATOM(bn, l, r);
	} else {
		bn = defaultfetchjoin_sht_LATOM(bn, l, r);
	}


#line 756 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_int ||
		   ATOMstorage(lht) == TYPE_flt) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = defaultfetchjoin_int_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = defaultfetchjoin_int_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = defaultfetchjoin_int_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = defaultfetchjoin_int_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = defaultfetchjoin_int_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = defaultfetchjoin_int_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = defaultfetchjoin_int_VATOM(bn, l, r);
	} else {
		bn = defaultfetchjoin_int_LATOM(bn, l, r);
	}


#line 759 "gdk_relop.mx"

	} else if (ATOMstorage(lht) == TYPE_lng ||
		   ATOMstorage(lht) == TYPE_dbl) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = defaultfetchjoin_lng_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = defaultfetchjoin_lng_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = defaultfetchjoin_lng_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = defaultfetchjoin_lng_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = defaultfetchjoin_lng_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = defaultfetchjoin_lng_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = defaultfetchjoin_lng_VATOM(bn, l, r);
	} else {
		bn = defaultfetchjoin_lng_LATOM(bn, l, r);
	}


#line 762 "gdk_relop.mx"

	} else if (l->hvarsized) {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = defaultfetchjoin_VATOM_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = defaultfetchjoin_VATOM_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = defaultfetchjoin_VATOM_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = defaultfetchjoin_VATOM_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = defaultfetchjoin_VATOM_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = defaultfetchjoin_VATOM_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = defaultfetchjoin_VATOM_VATOM(bn, l, r);
	} else {
		bn = defaultfetchjoin_VATOM_LATOM(bn, l, r);
	}


#line 764 "gdk_relop.mx"

	} else {
		
#line 733 "gdk_relop.mx"
	if (ATOMstorage(rtt) == TYPE_bte) {
		bn = defaultfetchjoin_LATOM_bte(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_sht) {
		bn = defaultfetchjoin_LATOM_sht(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_int) {
		bn = defaultfetchjoin_LATOM_int(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_flt) {
		bn = defaultfetchjoin_LATOM_flt(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_lng) {
		bn = defaultfetchjoin_LATOM_lng(bn, l, r);
	} else if (ATOMstorage(rtt) == TYPE_dbl) {
		bn = defaultfetchjoin_LATOM_dbl(bn, l, r);
	} else if (r->tvarsized) {
		bn = defaultfetchjoin_LATOM_VATOM(bn, l, r);
	} else {
		bn = defaultfetchjoin_LATOM_LATOM(bn, l, r);
	}


#line 766 "gdk_relop.mx"

	}


#line 1099 "gdk_relop.mx"

		}
		/* handle string trick */
		if (rtt != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
			if (r->batRestricted == BAT_READ) {
				assert(r->T->vheap->parentid > 0);
				BBPshare(r->T->vheap->parentid);
				bn->T->vheap = r->T->vheap;
			} else {
				bn->T->vheap = (Heap *) GDKzalloc(sizeof(Heap));
				if (bn->T->vheap == NULL) {
					BBPreclaim(bn);
					goto ready;
				}
				bn->T->vheap->parentid = bn->batCacheid;
				if (r->T->vheap->filename) {
					char *nme = BBP_physical(bn->batCacheid);

					bn->T->vheap->filename = (str) GDKmalloc(strlen(nme) + 12);
					if (bn->T->vheap->filename == NULL) {
						BBPreclaim(bn);
						goto ready;
					}
					GDKfilepath(bn->T->vheap->filename, NULL, nme, "theap");
				}
				if (HEAPcopy(bn->T->vheap, r->T->vheap) < 0) {
					BBPreclaim(bn);
					goto ready;
				}
			}
			bn->ttype = r->ttype;
			bn->tvarsized = 1;
			bn->T->width = r->T->width;
			bn->T->shift = r->T->shift;
		}
		/* if join columns are ordered, head inherits ordering */
		bn->hsorted = BATtordered(l) & BAThordered(r) & BAThordered(l);
		bn->hrevsorted = BATtordered(l) & BAThordered(r) & BAThrevordered(l);
	} else if (!BATtvoid(l)) {
		/* propagation of void columns in the result */
		int nondense = 0;
		int tpe = r->ttype;
		BUN dst;
		void *prev = NULL, *next = NULL;
		bit bntsorted = 0, bntrevsorted = 0;
		BATiter bni;

		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l)\n");

		if (ATOMstorage(tpe) == TYPE_str &&
		    /* GDK_ELIMDOUBLES(r->T->vheap) && */
		    (!rcount || (lcount << 3) > rcount)) {
			/* insert double-eliminated strings as ints */
			tpe = r->T->width == 1 ? TYPE_bte : (r->T->width == 2 ? TYPE_sht : (r->T->width == 4 ? TYPE_int : TYPE_lng));
		}
		bn = BATnew(TYPE_void, ATOMtype(tpe), estimate);
		if (bn == NULL)
			goto ready;
		ESTIDEBUG THRprintf(GDKout, "#BATfetchjoin: estimated resultsize: " BUNFMT "\n", lcount);

		bni = bat_iterator(bn);
		dst = BUNfirst(bn);

		if (ATOMstorage(tpe) == TYPE_bte) {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(bte,loc);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		Tbteput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(bte,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		Tbteput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(bte,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		Tbteput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(bte,loc): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			Tbteput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtloc(bni,dst);
				if (bn->tsorted && prev && bte_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && bte_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1163 "gdk_relop.mx"

		} else if (ATOMstorage(tpe) == TYPE_sht) {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(sht,loc);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		Tshtput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(sht,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		Tshtput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(sht,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		Tshtput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(sht,loc): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			Tshtput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtloc(bni,dst);
				if (bn->tsorted && prev && sht_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && sht_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1165 "gdk_relop.mx"

		} else if (ATOMstorage(tpe) == TYPE_int) {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(int,loc);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		Tintput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(int,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		Tintput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(int,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		Tintput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(int,loc): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			Tintput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtloc(bni,dst);
				if (bn->tsorted && prev && int_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && int_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1167 "gdk_relop.mx"

		} else if (ATOMstorage(tpe) == TYPE_flt) {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(flt,loc);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		Tfltput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(flt,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		Tfltput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(flt,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		Tfltput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(flt,loc): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			Tfltput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtloc(bni,dst);
				if (bn->tsorted && prev && flt_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && flt_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1169 "gdk_relop.mx"

		} else if (ATOMstorage(tpe) == TYPE_lng) {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(lng,loc);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		Tlngput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(lng,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		Tlngput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(lng,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		Tlngput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(lng,loc): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			Tlngput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtloc(bni,dst);
				if (bn->tsorted && prev && lng_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && lng_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1171 "gdk_relop.mx"

		} else if (ATOMstorage(tpe) == TYPE_dbl) {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(dbl,loc);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		Tdblput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(dbl,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		Tdblput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(dbl,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		Tdblput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(dbl,loc): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			Tdblput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtloc(bni,dst);
				if (bn->tsorted && prev && dbl_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && dbl_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1173 "gdk_relop.mx"

		} else if (r->tvarsized) {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(VATOM,var);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		TVATOMput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(VATOM,var);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		TVATOMput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(VATOM,var);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		TVATOMput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtvar(bni,dst);
			if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(VATOM,var): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			TVATOMput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtvar(bni,dst);
				if (bn->tsorted && prev && VATOM_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && VATOM_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1175 "gdk_relop.mx"

		} else {
			
#line 874 "gdk_relop.mx"
	if ((BATtordered(l) & BATtordered(r)) || \
	    (BATtordered(l) & BATtrevordered(r)) || \
	    (BATtrevordered(l) & BATtordered(r)) || \
	    (BATtrevordered(l) & BATtrevordered(r))) {
		/* will be set correctly once we're done */
		bn->tsorted = bn->trevsorted = 0;
	} else if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
		/* "string trick" => cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else if (r->ttype == TYPE_void) {
		/* cannot compare on the fly */
		bn->tsorted = bn->trevsorted = 0;
		derive_tail_properties = TRUE;
	} else {
		/* start optimistic & check on the fly */
		bn->tsorted = bn->trevsorted = 1;
	}
	if (BATtdense(l)) {
		/* dense => ordered, i.e., we did check the bounderies
		 * already above and we can do a "synchronized walk"
		 * through l & r */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && BATtdense(l)\n");
		
#line 771 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: densevoidfetchjoin(LATOM,loc);\n");
	r_cur = (BUN) (offset + * (oid *) BUNtloc(li, BUNfirst(l)));
	BATloop(l, l_cur, l_end) {
		TLATOMput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		r_cur++;
		dst++;
	}


#line 897 "gdk_relop.mx"

	} else if (BATtordered(l) || hitalways) {
		/* we did check the bounderies already above
		 * BATtordered(l) or simply "trust" hitalways */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && ( BATtordered(l) [== %d] || hitalways [== %d] )\n", BATtordered(l), (int)hitalways);
		
#line 791 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: orderedvoidfetchjoin(LATOM,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		r_cur = _yy;
		TLATOMput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}


#line 902 "gdk_relop.mx"

	} else {
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && !BATtvoid(l) && !BATtdense(l) && !BATtordered(l) && !hitalways\n");
		
#line 812 "gdk_relop.mx"
	ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(LATOM,loc);\n");
	BATloop(l, l_cur, l_end) {
		BUN _yy = (BUN) (offset + * (oid *) BUNtloc(li, l_cur));

		if (_yy < base || _yy >= end) {
			bntsorted = bn->tsorted;
			bntrevsorted = bn->trevsorted;
			BBPreclaim(bn);
			bn = NULL;
			nondense = 1;
			prev = next = NULL;
			break;
		}
		r_cur = _yy;
		TLATOMput(bn, Tloc(bn,dst));
		if (bn->tsorted || bn->trevsorted) {
			next = BUNtloc(bni,dst);
			if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
				bn->tsorted = 0;
			}
			if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
				bn->trevsorted = 0;
			}
			prev = next;
		}
		dst++;
	}
	if (nondense) {
		/* not (yet?) completely type-optimized ! */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: defaultvoidfetchjoin(LATOM,loc): discovered non-density, resuming with non-void head\n");
		bn = BATnew(BAThtype(l), ATOMtype(tpe), BATcount(l));
		if (bn == NULL)
			return bn;
		dst = BUNfirst(bn);
		bni = bat_iterator(bn);
		bn->tsorted = bntsorted;
		bn->trevsorted = bntrevsorted;
		BATloop(l, l_cur, l_end) {
			BUN _yy = (BUN) (offset + * (oid *) BUNtail(li, l_cur));

			if (_yy < base || _yy >= end) {
				continue;
			}
			r_cur = _yy;
			Hputvalue(bn, BUNhloc(bni, dst), BUNhead(li, l_cur), 1);
			TLATOMput(bn, BUNtloc(bni, dst));
			if (bn->tsorted || bn->trevsorted) {
				next = BUNtloc(bni,dst);
				if (bn->tsorted && prev && LATOM_cmp(bn,prev,next) > 0) {
					bn->tsorted = 0;
				}
				if (bn->trevsorted && prev && LATOM_cmp(bn,prev,next) < 0) {
					bn->trevsorted = 0;
				}
				prev = next;
			}
			dst++;
		}
	}


#line 905 "gdk_relop.mx"

	}


#line 1177 "gdk_relop.mx"

		}
		BATsetcount(bn, dst);
		ret = bn;
		goto bunins_failed;
	      bunins_failed:
		if (ret == NULL) {
			BBPreclaim(bn);
			goto ready;
		}
		/* handle string trick */
		if (tpe != r->ttype && ATOMstorage(r->ttype) == TYPE_str) {
			if (r->batRestricted == BAT_READ) {
				assert(r->T->vheap->parentid > 0);
				BBPshare(r->T->vheap->parentid);
				bn->T->vheap = r->T->vheap;
			} else {
				bn->T->vheap = (Heap *) GDKzalloc(sizeof(Heap));
				if (bn->T->vheap == NULL) {
					BBPreclaim(bn);
					ret = NULL;
					goto ready;
				}
				bn->T->vheap->parentid = bn->batCacheid;
				if (r->T->vheap->filename) {
					char *nme = BBP_physical(bn->batCacheid);

					bn->T->vheap->filename = (str) GDKmalloc(strlen(nme) + 12);
					if (bn->T->vheap->filename == NULL) {
						BBPreclaim(bn);
						ret = NULL;
						goto ready;
					}
					GDKfilepath(bn->T->vheap->filename, NULL, nme, "theap");
				}
				if (HEAPcopy(bn->T->vheap, r->T->vheap) < 0) {
					BBPreclaim(bn);
					ret = NULL;
					goto ready;
				}
			}
			bn->ttype = r->ttype;
			bn->tvarsized = 1;
			bn->T->width = r->T->width;
			bn->T->shift = r->T->shift;
		}
		if (nondense) {
			/* if join columns are ordered, head inherits
			 * ordering */
			bn->hsorted = BATtordered(l) & BAThordered(r) & BAThordered(l);
			bn->hrevsorted = BATtordered(l) & BAThordered(r) & BAThrevordered(l);
		} else {
			BATseqbase(bn, seqbase);
			if (seqbase != oid_nil)
				BATkey(bn, TRUE);
			bn->hsorted = 1;
			bn->hrevsorted = BATcount(bn) <= 1;
		}
	} else if (l->tseqbase != oid_nil) {
		/* execute using slice */
		BAT *v = r;
		oid lo_val = MAX(l->tseqbase, r->hseqbase);
		oid hi_val = MIN(l->tseqbase + lcount, r->hseqbase + rcount);
		BUN lo_pos = lo_val - r->hseqbase;
		BUN hi_pos = hi_val;

		if (v->H->type != TYPE_void) /* only create view when needed */
			v = BATmirror(VIEWhead(BATmirror(r)));
		if (hi_pos > r->hseqbase)
			hi_pos -= r->hseqbase;
		else
			hi_pos = 0;
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && BATtvoid(l) && l->tseqbase != oid_nil  =>  bn = BATslice(BATmirror(VIEWhead(BATmirror(r))), lo_pos=" BUNFMT ", hi_pos=" BUNFMT ");\n", lo_pos, hi_pos);

		bn = BATslice(v, lo_pos, hi_pos);
		if (seqbase != oid_nil)
			seqbase += lo_val - l->tseqbase;
		BATseqbase(bn, seqbase);
		if (v != r)
			BBPunfix(v->batCacheid);
	} else {
		/* nil join column => empty result */
		ALGODEBUG fprintf(stderr, "#BATfetchjoin: BAThvoid(l) && BATtvoid(l) && l->tseqbase == oid_nil\n");

		bn = BATnew(ATOMtype(l->htype), ATOMtype(r->ttype), 10);
		if (bn == NULL)
			goto ready;
		ESTIDEBUG THRprintf(GDKout, "#BATfetchjoin: estimated resultsize: %d\n", 10);
	}
	/* property propagation */
	if (BATcount(bn) == lcount) {
		ALIGNsetH(bn, l);	/* BAThkey(r), remember? */
	} else {
		if (hitalways | hitalways_check) {
			GDKerror("BATfetchjoin(%s,%s) does not hit always (|bn|=" BUNFMT " != " BUNFMT "=|l|) => can't use fetchjoin.\n", BATgetId(l), BATgetId(r), BATcount(bn), lcount);
			BBPreclaim(bn);
			ret = NULL;
			goto ready;
		}
		bn->hsorted = (BATcount(bn) <= 1) || l->hsorted;
		bn->hrevsorted = (BATcount(bn) <= 1) || l->hrevsorted;
	}
	bn->tsorted = bn->tsorted || \
	              (BATcount(bn) <= 1) || \
	              (BATtordered(l) & BATtordered(r)) || \
	              (BATtrevordered(l) & BATtrevordered(r));
	bn->trevsorted = bn->trevsorted || \
	                 (BATcount(bn) <= 1) || \
	                 (BATtordered(l) & BATtrevordered(r)) || \
	                 (BATtrevordered(l) & BATtordered(r));
	if (BATtkey(l)) {
		/* if BATtkey(l) elements of r match at most once */
		if (BATtordered(l) && BATcount(bn) == rcount) {
			ALIGNsetT(bn, r);
		} else {
			BATkey(BATmirror(bn), BATtkey(r));
		}
	}
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = r->T->nonil;

	if (derive_tail_properties) {
		/* invest in property check, since we cannot easily derive the
		 * result properties, but later operations might benefit from
		 * / depend on them
		 * Disable via command line option --debug=16777216
		 */
		JOINPROPCHK {
			if (bn) {
				BATderiveHeadProps(BATmirror(bn), 0);
			}
		}
	}
	ret = bn;
      ready:
	if (l != l_orig) {
		BBPreclaim(l);	/* was created as a temporary (slice) select on l */
	}
	ESTIDEBUG THRprintf(GDKout, "#BATfetchjoin: actual resultsize: " BUNFMT "\n", ret ? BATcount(ret) : 0);
	return ret;
}

BAT *
BATfetchjoin(BAT *l, BAT *r, BUN estimate)
{
	/* fetchjoin now implies that you assure no fetch misses (hitalways) */
	/* allows swapping of left and right input for faster processing */
	return batfetchjoin(l, r, estimate, TRUE, TRUE);
}

BAT *
BATleftfetchjoin(BAT *l, BAT *r, BUN estimate)
{
	/* fetchjoin now implies that you assure no fetch misses (hitalways) */
	/* do not swap left and right input, and hence maintain order
	 * of left head in result */
	return batfetchjoin(l, r, estimate, FALSE, TRUE);
}

/*
 * This routine does the join optimization.
 */
static BAT *
batjoin(BAT *l, BAT *r, BUN estimate, bit swap)
{
	size_t lsize, rsize, mem_size = MT_npages() * MT_pagesize() / (GDKnr_threads ? GDKnr_threads : 1);
	BUN i, lcount, rcount;
	bit lfetch, rfetch, must_hash;
	lng logr, logl;

	ERRORcheck(l == NULL, "BATjoin: invalid left operand");
	ERRORcheck(r == NULL, "BATjoin: invalid right operand");
	ERRORcheck(TYPEerror(l->ttype, r->htype), "BATjoin: type conflict\n");
	lcount = BATcount(l);
	rcount = BATcount(r);

	if (lcount == 0 || rcount == 0 ||
	    (l->ttype == TYPE_void && l->tseqbase == oid_nil) ||
	    (r->htype == TYPE_void && r->hseqbase == oid_nil)) {
		BAT *bn;

		
#line 958 "gdk_relop.mx"
		ALGODEBUG fprintf(stderr, "# BATjoin: |l|==0 or |r|==0 or tail(l)==NIL or head(r)==NIL => empty result\n");
#if 0
		if (hitalways|hitalways_check && lcount > 0) {
			GDKerror("BATfetchjoin(%s,%s) does not hit always (|bn|=0 != "BUNFMT"=|l|) => can't use fetchjoin.\n", BATgetId(l), BATgetId(r), lcount);
			return NULL;
		}
#endif
		bn = BATnew(l->htype, r->ttype, 0);
		if (bn == NULL)
			return NULL;
		BATkey(bn, TRUE);
		BATkey(BATmirror(bn), TRUE);
		if (bn->htype == TYPE_void || bn->htype == TYPE_oid) {
			BATseqbase(bn, l->htype == TYPE_void ? l->hseqbase : 0);
			bn->hdense = bn->hseqbase != oid_nil;
		}
		if (bn->ttype == TYPE_void || bn->ttype == TYPE_oid) {
			BATseqbase(BATmirror(bn), r->ttype == TYPE_void ? r->tseqbase : 0);
			bn->tdense = bn->tseqbase != oid_nil;
		}
		return bn;


#line 1358 "gdk_relop.mx"

	}
	if (BATtdense(l) && BAThdense(r) && l->tseqbase == r->hseqbase &&
	    lcount == rcount &&
	    l->batRestricted == BAT_READ && r->batRestricted == BAT_READ)
		return VIEWcreate(l, r);
	/*
	 * collect statistics that help us decide what to do
	 */
	lsize = lcount * (Hsize(l) + Tsize(l)) + (l->H->vheap ? l->H->vheap->size : 0) + (l->T->vheap ? l->T->vheap->size : 0) + 2 * lcount * sizeof(BUN);
	rsize = rcount * (Hsize(r) + Tsize(r)) + (r->H->vheap ? r->H->vheap->size : 0) + (r->T->vheap ? r->T->vheap->size : 0) + 2 * rcount * sizeof(BUN);
	for (logr = 4, i = rcount; i > 0; logr++)
		i >>= 1;
	for (logl = 4, i = lcount; i > 0; logl++)
		i >>= 1;

	rfetch = BAThdense(r);
	lfetch = BATtdense(l);
	/* in case of fetchjoin, make sure we propagate a non-join
	 * void column */
	if (lfetch && rfetch) {
		if (BAThvoid(l) && !BATtvoid(r))
			lfetch = 0;
		if (swap && BATtvoid(r) && !BAThvoid(l))
			rfetch = 0;
	}
	/* in case of fetchjoin, make sure we exploit sortedness for
	 * sequential access */
	if (lfetch && rfetch) {
		if (BATtordered(l) && !BAThordered(r))
			lfetch = 0;
		if (swap && BAThordered(r) && !BATtordered(l))
			rfetch = 0;
	}
	must_hash = swap && rsize > lsize ? l->T->hash == NULL : r->H->hash == NULL;
	/*
	 * Inner input out of memory => sort-merge-join performs
	 * better than hash-join or even random-access fetch-join.
	 */
	if (((swap && MIN(lsize, rsize) > mem_size) ||
	     (!swap && rsize > mem_size)) &&
	    !(BATtordered(l) & BAThordered(r))) {
		/* inner input out of memory, but not both sorted
		 * (sequential-access fetch/merge handled by special
		 * cases below) */
		if (BATtordered(l) || swap) {
			/* left tail already sorted (i.e., no re-order
			 * required) or left-order-preserving not
			 * required (i.e., re-order allowed) */
			BAT *ls, *rs, *j;

			/* if not yet sorted on tail, sort left input on tail */
			ls = BATtordered(l) ? l : BATmirror(BATsort(BATmirror(l)));
			ERRORcheck(ls == NULL, "BATjoin: BATmirror(BATsort(BATmirror(l))) failed");
			/* if not yet sorted on head, sort right input
			 * on head */
			rs = BAThordered(r) ? r : BATsort(r);
			ERRORcheck(rs == NULL, "BATjoin: BATsort(r) failed");
			if (swap && rsize > lsize) {
				/* left-order-preserving not required:
				 * user smaller input as inner
				 * (right) */
				/* (not sure, though, whether this
				 * makes a difference with merge-join
				 * ...) */
				ALGODEBUG fprintf(stderr, "#BATjoin: BATmirror(BATmergejoin(BATmirror(BATsort(r)), BATsort(BATmirror(l)), " BUNFMT "));\n", estimate);
				j = BATmirror(batmergejoin(BATmirror(rs), BATmirror(ls), estimate, swap, NULL));
				ERRORcheck(j == NULL, "BATjoin: BATmirror(batmergejoin(BATmirror(rs), BATmirror(ls), estimate, swap, NULL)) failed");
			} else {
				/* left-order-preserving required, or
				 * inner (right) input is smaller
				 * one */
				ALGODEBUG fprintf(stderr, "#BATjoin: BATmergejoin(BATmirror(BATsort(BATmirror(l))), BATsort(r), " BUNFMT "));\n", estimate);
				j = batmergejoin(ls, rs, estimate, swap, NULL);
				ERRORcheck(j == NULL, "BATjoin: batmergejoin(ls, rs, estimate, swap, NULL) failed");
			}
			if (ls != l) {
				/* release temp. tail-ordered copy of
				 * left input */
				BBPunfix(ls->batCacheid);
			}
			if (rs != r) {
				/* release temp. head-ordered copy of
				 * right input */
				BBPunfix(rs->batCacheid);
			}
			return j;
		} else
			/* as of here, left order must be preserved /
			 * restored */
		if (BAThordered(l) && l->htype < TYPE_str) {
			/* left head sorted (i.e., left order can be
			 * restored with simple (stable) sort of join
			 * result), provided the sort is fast (i.e.,
			 * not on varsized types) */
			BAT *ls, *rs, *jj, *j;

			ALGODEBUG fprintf(stderr, "#BATjoin: BAT[s]sort(BATmergejoin(BATmirror(BAT[s]sort(BATmirror(l))), BATsort(r), " BUNFMT "));\n", estimate);
			/* sort left input on tail, use stable sort to
			 * maintain original order of duplicate head
			 * values */
			ls = BAThkey(l) ? BATmirror(BATsort(BATmirror(l))) : BATmirror(BATssort(BATmirror(l)));
			ERRORcheck(ls == NULL, "BATjoin: BATmirror(BAT[s]sort(BATmirror(l))) failed");
			/* if not yet sorted on head, sort right input
			 * on head */
			rs = BAThordered(r) ? r : BATsort(r);
			ERRORcheck(rs == NULL, "BATjoin: BATsort(r) failed");
			/* perform merge join */
			jj = batmergejoin(ls, rs, estimate, swap, NULL);
			ERRORcheck(jj == NULL, "BATjoin: batmergejoin(ls, rs, estimate, swap, NULL) failed");
			/* release temp. tail-ordered copy of left input */
			BBPunfix(ls->batCacheid);
			ls = NULL;
			if (rs != r) {
				/* release temp. head-ordered copy of
				 * right input */
				BBPunfix(rs->batCacheid);
				rs = NULL;
			}
			/* sort join result on head to restore
			 * physical left-input-order; use stable sort
			 * to maintain original order of duplicate
			 * head values */
			j = BAThkey(l) ? BATsort(jj) : BATssort(jj);
			ERRORcheck(j == NULL, "BATjoin: BAT[s]sort(jj) failed");
			/* release temp. unordered join result */
			BBPunfix(jj->batCacheid);
			jj = NULL;
			return j;
		} else {
			/* - separate left head & tail using BATmark
			 * - sort left tail
			 * - sort right head
			 * - merge-join left (tail) & right (head)
			 * - sort join result on BATmark-generated
			 *   left OIDs to restore left order
			 * - re-add left head with seq. access fetchjoin
			 */
			BAT *lh, *lt, *ls, *rs, *jj, *js, *j;

			ALGODEBUG fprintf(stderr, "#BATjoin: BATmirror(batfetchjoin(BATmirror(BATsort(BATmergejoin(BATmirror(BATsort(BATmark(BATmirror(l),0))), BATsort(r), " BUNFMT "))), BATmirror(BATmark(l,0))));\n", estimate);
			/* separate left head & tail using BATmark */
			lh = BATmark(l, 0);
			ERRORcheck(lh == NULL, "BATjoin: BATmark(l,0) failed");
			lt = BATmirror(BATmark(BATmirror(l), 0));
			ERRORcheck(lt == NULL, "BATjoin: BATmirror(BATmark(BATmirror(l),0)) failed");
			/* sort left tail */
			ls = BATmirror(BATsort(BATmirror(lt)));
			ERRORcheck(ls == NULL, "BATjoin: BATmirror(BATsort(BATmirror(lt))) failed");
			/* release temp. unsorted left tail */
			BBPunfix(lt->batCacheid);
			lt = NULL;
			/* if not yet sorted on head, sort right input on head */
			rs = BAThordered(r) ? r : BATsort(r);
			ERRORcheck(rs == NULL, "BATjoin: BATsort(r) failed");
			/* perform merge join */
			jj = batmergejoin(ls, rs, estimate, swap, NULL);
			ERRORcheck(jj == NULL, "BATjoin: batmergejoin(ls, rs, estimate, swap, NULL) failed");
			/* release temp. ordered copy of left tail */
			BBPunfix(ls->batCacheid);
			ls = NULL;
			if (rs != r) {
				/* release temp. head-ordered copy of
				 * right input */
				BBPunfix(rs->batCacheid);
				rs = NULL;
			}
			/* sort join result on head to restore
			 * physical left-input-order */
			js = BATsort(jj);
			ERRORcheck(js == NULL, "BATjoin: BATsort(jj) failed");
			/* release temp. unordered join result */
			BBPunfix(jj->batCacheid);
			jj = NULL;
			/* restore original left head values */
			j = BATmirror(batfetchjoin(BATmirror(js), BATmirror(lh), BATcount(js), swap, TRUE));
			ERRORcheck(j == NULL, "BATjoin: BATmirror(batfetchjoin(BATmirror(js), BATmirror(lh), BATcount(js), swap, TRUE)) failed");
			/* release temp.sorted join result */
			BBPunfix(js->batCacheid);
			js = NULL;
			/* release temp. copy of left head */
			BBPunfix(lh->batCacheid);
			lh = NULL;
			return j;
		}
	}
	/*
	 * In special cases (equal join columns, void join columns, or
	 * ordered join columns), we take special action.
	 */
	if (swap && lfetch && !(rfetch && lcount <= rcount)) {
		ALGODEBUG fprintf(stderr, "#BATjoin: BATmirror(BATfetchjoin(BATmirror(r), BATmirror(l), " BUNFMT "));\n", estimate);

		return BATmirror(batfetchjoin(BATmirror(r), BATmirror(l), estimate, TRUE, FALSE));
	} else if (rfetch) {
		ALGODEBUG fprintf(stderr, "#BATjoin: BATfetchjoin(l, r, " BUNFMT ");\n", estimate);

		return batfetchjoin(l, r, estimate, swap, FALSE);
	}
	/*
	 * If both are ordered we do merge-join, or if hash-join is
	 * not possible right away and one input is ordered and the
	 * other is much smaller, we do nested loop binary search
	 * (both implemented by BATmergejoin).
	 */
	if ((BATtordered(l) & BAThordered(r)) ||
	    (must_hash &&
	     ((BATtordered(l) &&
	       ((lng) lcount > logl * (lng) rcount) &&
	       swap) ||
	      (BAThordered(r) &&
	       ((lng) rcount > logr * (lng) lcount))))) {
		ALGODEBUG fprintf(stderr, "#BATjoin: BATmergejoin(l,r," BUNFMT ");\n", estimate);

		return batmergejoin(l, r, estimate, swap, NULL);
	}
	/*
	 * hash join: the bread&butter join of monet
	 */
	/* Simple rule, always build hash on the smallest */
	if (swap && rsize > lsize) {
		ALGODEBUG fprintf(stderr, "#BATjoin: BATmirror(BAThashjoin(BATmirror(r), BATmirror(l)," BUNFMT "));\n", estimate);

		return BATmirror(BAThashjoin(BATmirror(r), BATmirror(l), estimate));
	}
	ALGODEBUG fprintf(stderr, "#BATjoin: BAThashjoin(l,r," BUNFMT ");\n", estimate);

	return BAThashjoin(l, r, estimate);
}

BAT *
BATjoin(BAT *l, BAT *r, BUN estimate)
{
	/* allows swapping of left and right input for faster processing */
	BAT *b = batjoin(l, r, estimate, TRUE);

	/* invest in property check, since we cannot easily derive the
	 * result properties, but later operations might benefit from
	 * / depend on them
	 * Disable via command line option --debug=16777216
	 */
	JOINPROPCHK {
		if (b) {
			BATderiveProps(b, 0);
		}
	}

	return b;
}

BAT *
BATleftjoin(BAT *l, BAT *r, BUN estimate)
{
	/* do not swap left and right input, and hence maintain order
	 * of left head in result */
	BAT *b = batjoin(l, r, estimate, FALSE);

	/* invest in property check, since we cannot easily derive the
	 * result properties, but later operations might benefit from
	 * / depend on them
	 * Disable via command line option --debug=16777216
	 */
	JOINPROPCHK {
		if (b) {
			BATderiveProps(b, 0);
		}
	}
	return b;
}

/*
 * @+  Outerjoin
 * The left outerjoin between two BAT is also supported. The code is
 * identical to the hashjoin algorithm with the extension to insert a BUN
 * if no match can be found.
 */


#line 1658 "gdk_relop.mx"
/*
 * The baseline join algorithm creates a hash on the smallest element and
 * probes it using the larger one. [TODO]
 */
BAT *
BATouterjoin(BAT *l, BAT *r, BUN estimate)
{
	BAT *bn = NULL;
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	
#line 60 "gdk_relop.mx"
	ERRORcheck(l == NULL, "BATouterjoin: invalid left operand");
	ERRORcheck(r == NULL, "BATouterjoin: invalid right operand");
	ERRORcheck(TYPEerror(l->ttype, r->htype), "BATouterjoin: type conflict\n");


#line 1669 "gdk_relop.mx"

	if (BAThkey(r) && (estimate == BUN_NONE || estimate < BATcount(l)))
		estimate = BATcount(l);
	if (BAThdense(l) && BAThkey(r)) {
		bn = BATnew(TYPE_void, ATOMtype(r->ttype), estimate);
		if (bn == NULL)
			return bn;
		ESTIDEBUG THRprintf(GDKout, "#BATouterjoin: estimated resultsize: " BUNFMT "\n", estimate);

		BATseqbase(bn, l->hseqbase);
	}
	if (BAThdense(r) == FALSE && BAThordered(r)) {
		/* use the merge-join; it takes care of the rest */
		ALGODEBUG fprintf(stderr, "#BATouterjoin: mergejoin(l, r, bn, ATOMnilptr(r->ttype), estimate);\n");

		bn = mergejoin(l, r, bn, ATOMnilptr(r->ttype), estimate, NULL);
		ESTIDEBUG THRprintf(GDKout, "#BATouterjoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

		/* invest in property check, since we cannot easily
		 * derive the result properties, but later operations
		 * might benefit from / depend on them
		 * Disable via command line option --debug=16777216
		 */
		JOINPROPCHK {
			if (bn)
				BATderiveProps(bn, 0);
		}

		return bn;
	} else if (bn == NULL) {
		
#line 170 "gdk_relop.mx"
	{
		BUN _estimate = estimate;

		
#line 64 "gdk_relop.mx"
	if ( _estimate == BUN_NONE) {
		BUN _lcount = BATcount(l);
		BUN _rcount = BATcount(r);
		BUN _slices = 0;

		/* limit estimate with simple bounds first; only spend
		 * effort if the join result might be big */
		if (JOIN_EQ == JOIN_EQ) {
			if (l->tkey)
				 _estimate = r->hkey ? MIN(_rcount, _lcount) : _rcount;
			else if (r->hkey)
				 _estimate = _lcount;
		}
		if ( _estimate == BUN_NONE) {
			BUN _heuristic = MIN(_lcount, _rcount);

			if (_heuristic <= BUN_MAX / 3) {
				_heuristic *= 3;
				if (_heuristic <= (1 << SAMPLE_TRESHOLD_LOG))
					 _estimate = _heuristic;
			}
		}
		if ( _estimate == BUN_NONE) {
			BUN _idx;

			for (_idx = _lcount; _idx > 0; _idx >>= 1)
				_slices++;
		}
		if (_slices > SAMPLE_TRESHOLD_LOG) {
			/* use cheapo sampling by taking a number of
			 * slices and joining those with the algo */
			BUN _idx = 0, _tot = 0, _step, _lo, _avg, _sample, *_cnt;
			BAT *_tmp1 = l, *_tmp2, *_tmp3 = NULL;

			_step = _lcount / (_slices -= SAMPLE_TRESHOLD_LOG);
			_sample = _slices * SAMPLE_SLICE_SIZE;
			_cnt = GDKmalloc(_slices * sizeof(BUN));
			if (_cnt == NULL)
				return NULL;
			for (_lo = 0; _idx < _slices; _lo += _step) {
				BUN _size = 0, _hi = _lo + SAMPLE_SLICE_SIZE;

				l = BATslice(_tmp1, _lo, _hi);	/* slice keeps all parent properties */
				if (l == NULL) {
					GDKfree(_cnt);
					return NULL;
				}
				_tmp2 =  BATouterjoin(l,r,BUN_NONE);	/*  BATouterjoin(l,r,BUN_NONE) = e.g. BATXjoin(l,r) */
				if (_tmp2) {
					_size = BATcount(_tmp2);
					BBPreclaim(_tmp2);
				}
				_tot += (_cnt[_idx++] = _size);
				BBPreclaim(l);
			}
			/* do outlier detection on sampling results;
			 * this guards against skew */
			if (JOIN_EQ == JOIN_EQ) {
				for (_avg = _tot / _slices, _idx = 0; _idx < _slices; _idx++) {
					BUN _diff = _cnt[_idx] - _avg;

					if (_avg > _cnt[_idx])
						_diff = _avg - _cnt[_idx];
					if (_diff > MAX(SAMPLE_SLICE_SIZE, _avg))
						break;
				}
				if (_idx < _slices) {
					/* outliers detected, compute
					 * a real sample on at most 1%
					 * of the data */
					_sample = MIN(_lcount / 100, (1 << SAMPLE_TRESHOLD_LOG) / 3);
					_tmp2 = BATsample(_tmp1, _sample);
					if (_tmp2) {
						_tmp3 = BATjoin(_tmp2, r, BUN_NONE);	/* might be expensive */
						if (_tmp3) {
							_tot = BATcount(_tmp3);
							BBPreclaim(_tmp3);
						}
						BBPreclaim(_tmp2);
					}
					if (_tmp3 == NULL) {
						GDKfree(_cnt);
						return NULL;
					}
				}
			}
			GDKfree(_cnt);
			/* overestimate always by 5% */
			{
				double _d = (double) (((lng) _tot) * ((lng) _lcount)) / (0.95 * (double) _sample);
				if (_d < (double) BUN_MAX)
					 _estimate = (BUN) _d;
				else
					 _estimate = BUN_MAX;
			}
			l = _tmp1;
		} else {
			BUN _m = MIN(_lcount,_rcount);
			if (_m <= BUN_MAX / 32)
				_m *= 32;
			else
				_m = BUN_MAX;
			 _estimate = MIN(_m,MAX(_lcount,_rcount));
		}
	}


#line 173 "gdk_relop.mx"

		bn = BATnew(BAThtype(l), BATttype(r), _estimate);
		if (bn == NULL)
			return bn;
	}


#line 1699 "gdk_relop.mx"

	}

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	if (BAThdense(r)) {
		/* positional algorithm: hash on void column would
		 * give error and is stupid */
		ptr nilt = ATOMnilptr(r->ttype);
		bit nonil = TRUE;
		BUN p, q, w, s = BUNfirst(bn);

		BATloop(l, p, q) {
			oid v = *(oid *) BUNtail(li, p);
			ptr t = nilt;

			if (v != oid_nil) {
				BUNfndVOID(w, ri, &v);
				if (w != BUN_NONE)
					t = BUNtail(ri, w);
			}
			nonil &= (t != nilt);
			bunfastins_nocheck(bn, s, BUNhead(li, p), t, Hsize(bn), Tsize(bn));
			s++;
		}
		bn->tsorted = BATtordered(l) && BATtordered(r) && nonil;
		bn->trevsorted = BATcount(bn) <= 1;
	} else {
		/* hash based algorithm (default) */
		int any = ATOMstorage(r->htype);

		if (BATprepareHash(r)) {
			BBPreclaim(bn);
			return NULL;
		}
		if (BAThkey(r)) {
			
#line 1747 "gdk_relop.mx"
		switch (any) {
		case TYPE_bte:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	 BUN s = BUNfirst(bn);

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, bte))
			HASHloop_bte(ri, r->H->hash, xx, v) {
				bunfastins_nocheck_inc(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_nocheck_inc(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1749 "gdk_relop.mx"

		case TYPE_sht:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	 BUN s = BUNfirst(bn);

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, sht))
			HASHloop_sht(ri, r->H->hash, xx, v) {
				bunfastins_nocheck_inc(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_nocheck_inc(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1751 "gdk_relop.mx"

		case TYPE_int:
		case TYPE_flt:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	 BUN s = BUNfirst(bn);

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, int))
			HASHloop_int(ri, r->H->hash, xx, v) {
				bunfastins_nocheck_inc(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_nocheck_inc(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1754 "gdk_relop.mx"

		case TYPE_dbl:
		case TYPE_lng:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	 BUN s = BUNfirst(bn);

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, lng))
			HASHloop_lng(ri, r->H->hash, xx, v) {
				bunfastins_nocheck_inc(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_nocheck_inc(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1757 "gdk_relop.mx"

		case TYPE_str:
			if (l->T->vheap->hashash) {
				
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	 BUN s = BUNfirst(bn);

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!atom_EQ(v, nilh, any))
			HASHloop_str_hv(ri, r->H->hash, xx, v) {
				bunfastins_nocheck_inc(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_nocheck_inc(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1760 "gdk_relop.mx"

			}
			/* fall through */
		default:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	 BUN s = BUNfirst(bn);

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!atom_EQ(v, nilh, any))
			HASHloop_any(ri, r->H->hash, xx, v) {
				bunfastins_nocheck_inc(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_nocheck_inc(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1764 "gdk_relop.mx"

		}


#line 1742 "gdk_relop.mx"

		} else {
			
#line 1747 "gdk_relop.mx"
		switch (any) {
		case TYPE_bte:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, bte))
			HASHloop_bte(ri, r->H->hash, xx, v) {
				bunfastins_check(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_check(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1749 "gdk_relop.mx"

		case TYPE_sht:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, sht))
			HASHloop_sht(ri, r->H->hash, xx, v) {
				bunfastins_check(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_check(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1751 "gdk_relop.mx"

		case TYPE_int:
		case TYPE_flt:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, int))
			HASHloop_int(ri, r->H->hash, xx, v) {
				bunfastins_check(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_check(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1754 "gdk_relop.mx"

		case TYPE_dbl:
		case TYPE_lng:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!simple_EQ(v, nilh, lng))
			HASHloop_lng(ri, r->H->hash, xx, v) {
				bunfastins_check(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_check(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1757 "gdk_relop.mx"

		case TYPE_str:
			if (l->T->vheap->hashash) {
				
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!atom_EQ(v, nilh, any))
			HASHloop_str_hv(ri, r->H->hash, xx, v) {
				bunfastins_check(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_check(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1760 "gdk_relop.mx"

			}
			/* fall through */
		default:
			
#line 1635 "gdk_relop.mx"
{
	ptr v, nilh = ATOMnilptr(r->htype), nilt = ATOMnilptr(r->ttype);
	BUN xx;
	BUN p, q;
	

	BATloop(l, p, q) {
		BUN i = 0;

		v = (ptr) BUNtail(li, p);
		if (!atom_EQ(v, nilh, any))
			HASHloop_any(ri, r->H->hash, xx, v) {
				bunfastins_check(bn, s, BUNhead(li, p), BUNtail(ri, xx));
				i++;
			}
		if (i == 0) {
			bunfastins_check(bn, s, BUNhead(li, p), nilt);
		}
	}
}
break;


#line 1764 "gdk_relop.mx"

		}


#line 1744 "gdk_relop.mx"

		}


#line 1768 "gdk_relop.mx"
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
	}
	/* set sorted flags by hand, because we used BUNfastins() */
	if (r->hkey) {
		ALIGNsetH(bn, l);	/* always 1 hit, so columns are equal */
	} else {
		bn->hsorted = BAThordered(l);
		bn->hrevsorted = BAThrevordered(l);
	}
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = FALSE;
	ESTIDEBUG THRprintf(GDKout, "#BATouterjoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

	/* invest in property check, since we cannot easily derive the
	 * result properties, but later operations might benefit from
	 * / depend on them
	 * Disable via command line option --debug=16777216
	 */
	JOINPROPCHK {
		if (bn)
			BATderiveHeadProps(bn, 0);
	}

	return bn;

      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/*
 * @+ ThetaJoin
 * Current predicates supported are: JOIN_EQ, JOIN_LT,
 * JOIN_GE, JOIN_LE and JOIN_GT. The JOIN_EQ will pass the control to the
 * normal BATjoin equijoin. The is and index-based join: if an index
 * is not present, it will be created on the smallest relation.
 *
 * We do lots of code-inlining: first of all on join type (4), and
 * furthermore on left-tail (equal right-head) type (5), which are the
 * join columns.  We factor out more by splitting on storage strategy
 * (variable-sized/fixed-size) of both the left-head, and right tail
 * columns (2*2).
 *
 * In the end, this results in 4*5*2*2 = 80 different inner loops.
 */
BAT *
BATthetajoin(BAT *l, BAT *r, int op, BUN estimate)
{
	BUN _lcount = BATcount(l);
	BUN _rcount = BATcount(r);
	BUN _estimate = (BUN) MIN((lng) _lcount * _rcount, BUN_MAX);

	assert(_estimate <= BUN_MAX);
	
#line 60 "gdk_relop.mx"
	ERRORcheck(l == NULL, "BATthetajoin: invalid left operand");
	ERRORcheck(r == NULL, "BATthetajoin: invalid right operand");
	ERRORcheck(TYPEerror(l->ttype, r->htype), "BATthetajoin: type conflict\n");


#line 1822 "gdk_relop.mx"

	if (estimate < _estimate)
		_estimate = estimate;
	if (op == JOIN_EQ) {
		/* exploit all equi-join optimizations */
		ALGODEBUG fprintf(stderr, "#BATthetajoin(l,r,JOIN_EQ): BATjoin(l, r);\n");

		return BATjoin(l, r, _estimate);
	}
	return BATnlthetajoin(l, r, op, _estimate);
}

/* nested loop join; finally MonetDB can enjoy the virtues of this
 * algorithm as well! */


#line 1941 "gdk_relop.mx"

#line 1933 "gdk_relop.mx"
	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_gt_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	bte *la = (bte *) BUNtloc(lbi, li);
	bte *ra = (bte *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		bte v = la[li];
		oid o;

		if (v == bte_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1933 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_gt_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	sht *la = (sht *) BUNtloc(lbi, li);
	sht *ra = (sht *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		sht v = la[li];
		oid o;

		if (v == sht_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1934 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_gt_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	int *la = (int *) BUNtloc(lbi, li);
	int *ra = (int *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		int v = la[li];
		oid o;

		if (v == int_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1935 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_gt_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	lng *la = (lng *) BUNtloc(lbi, li);
	lng *ra = (lng *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		lng v = la[li];
		oid o;

		if (v == lng_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1936 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_gt_flt(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	flt *la = (flt *) BUNtloc(lbi, li);
	flt *ra = (flt *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		flt v = la[li];
		oid o;

		if (v == flt_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1937 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_gt_dbl(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	dbl *la = (dbl *) BUNtloc(lbi, li);
	dbl *ra = (dbl *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		dbl v = la[li];
		oid o;

		if (v == dbl_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v > ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v > ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1938 "gdk_relop.mx"



#line 1941 "gdk_relop.mx"


#line 1933 "gdk_relop.mx"
	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_ge_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	bte *la = (bte *) BUNtloc(lbi, li);
	bte *ra = (bte *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		bte v = la[li];
		oid o;

		if (v == bte_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1933 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_ge_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	sht *la = (sht *) BUNtloc(lbi, li);
	sht *ra = (sht *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		sht v = la[li];
		oid o;

		if (v == sht_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1934 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_ge_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	int *la = (int *) BUNtloc(lbi, li);
	int *ra = (int *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		int v = la[li];
		oid o;

		if (v == int_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1935 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_ge_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	lng *la = (lng *) BUNtloc(lbi, li);
	lng *ra = (lng *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		lng v = la[li];
		oid o;

		if (v == lng_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1936 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_ge_flt(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	flt *la = (flt *) BUNtloc(lbi, li);
	flt *ra = (flt *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		flt v = la[li];
		oid o;

		if (v == flt_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1937 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_ge_dbl(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	dbl *la = (dbl *) BUNtloc(lbi, li);
	dbl *ra = (dbl *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		dbl v = la[li];
		oid o;

		if (v == dbl_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v >= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v >= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1938 "gdk_relop.mx"



#line 1942 "gdk_relop.mx"


#line 1933 "gdk_relop.mx"
	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_lt_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	bte *la = (bte *) BUNtloc(lbi, li);
	bte *ra = (bte *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		bte v = la[li];
		oid o;

		if (v == bte_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1933 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_lt_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	sht *la = (sht *) BUNtloc(lbi, li);
	sht *ra = (sht *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		sht v = la[li];
		oid o;

		if (v == sht_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1934 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_lt_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	int *la = (int *) BUNtloc(lbi, li);
	int *ra = (int *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		int v = la[li];
		oid o;

		if (v == int_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1935 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_lt_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	lng *la = (lng *) BUNtloc(lbi, li);
	lng *ra = (lng *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		lng v = la[li];
		oid o;

		if (v == lng_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1936 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_lt_flt(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	flt *la = (flt *) BUNtloc(lbi, li);
	flt *ra = (flt *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		flt v = la[li];
		oid o;

		if (v == flt_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1937 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_lt_dbl(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	dbl *la = (dbl *) BUNtloc(lbi, li);
	dbl *ra = (dbl *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		dbl v = la[li];
		oid o;

		if (v == dbl_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v < ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v < ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1938 "gdk_relop.mx"



#line 1943 "gdk_relop.mx"


#line 1933 "gdk_relop.mx"
	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_le_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	bte *la = (bte *) BUNtloc(lbi, li);
	bte *ra = (bte *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		bte v = la[li];
		oid o;

		if (v == bte_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1933 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_le_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	sht *la = (sht *) BUNtloc(lbi, li);
	sht *ra = (sht *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		sht v = la[li];
		oid o;

		if (v == sht_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1934 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_le_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	int *la = (int *) BUNtloc(lbi, li);
	int *ra = (int *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		int v = la[li];
		oid o;

		if (v == int_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1935 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_le_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	lng *la = (lng *) BUNtloc(lbi, li);
	lng *ra = (lng *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		lng v = la[li];
		oid o;

		if (v == lng_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1936 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_le_flt(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	flt *la = (flt *) BUNtloc(lbi, li);
	flt *ra = (flt *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		flt v = la[li];
		oid o;

		if (v == flt_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1937 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_le_dbl(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	dbl *la = (dbl *) BUNtloc(lbi, li);
	dbl *ra = (dbl *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		dbl v = la[li];
		oid o;

		if (v == dbl_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v <= ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v <= ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1938 "gdk_relop.mx"



#line 1944 "gdk_relop.mx"


#line 1933 "gdk_relop.mx"
	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_eq_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	bte *la = (bte *) BUNtloc(lbi, li);
	bte *ra = (bte *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		bte v = la[li];
		oid o;

		if (v == bte_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1933 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_eq_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	sht *la = (sht *) BUNtloc(lbi, li);
	sht *ra = (sht *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		sht v = la[li];
		oid o;

		if (v == sht_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1934 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_eq_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	int *la = (int *) BUNtloc(lbi, li);
	int *ra = (int *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		int v = la[li];
		oid o;

		if (v == int_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1935 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_eq_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	lng *la = (lng *) BUNtloc(lbi, li);
	lng *ra = (lng *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		lng v = la[li];
		oid o;

		if (v == lng_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1936 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_eq_flt(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	flt *la = (flt *) BUNtloc(lbi, li);
	flt *ra = (flt *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		flt v = la[li];
		oid o;

		if (v == flt_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1937 "gdk_relop.mx"

	
#line 1864 "gdk_relop.mx"
static int
nlthetajoin_eq_dbl(BAT *bn, BAT *l, BAT *r)
{
	BATiter lbi = bat_iterator(l);
	BATiter rbi = bat_iterator(r);
	oid *hdst = (oid *) Hloc(bn, BUNfirst(bn)), *tdst = (oid *) Tloc(bn, BUNfirst(bn));
	BUN cur = BUNfirst(bn);
	BUN lim = BATcapacity(bn);
	BUN li = BUNfirst(l), lhi = BUNlast(l);
	BUN ri = BUNfirst(r), rhi = MAX(ri + 8, BUNlast(r)) - 8;
	dbl *la = (dbl *) BUNtloc(lbi, li);
	dbl *ra = (dbl *) BUNhloc(rbi, ri);
	oid *rt = (oid *) (r->ttype == TYPE_void ? 0 : BUNtloc(rbi, ri));

	for (ri = li = 0; li < lhi; li++, ri = 0) {
		oid off = r->ttype == TYPE_void ? r->tseqbase : 0;
		BUN len = cur;
		dbl v = la[li];
		oid o;

		if (v == dbl_nil)
			continue;

		/* unroll 8 times, factor out cur->l and memory
		 * re-allocation checking */
		while (1) {
			if (cur + 8 >= lim) {
				BATsetcount(bn, cur);
				if (BATextend(bn, 8 + (BUN) (BATcount(bn) * (((dbl) lhi) / (li + 1)))) == NULL)
					return 1;
				lim = BATcapacity(bn);
				hdst = (oid *) Hloc(bn, BUNfirst(bn));
				tdst = (oid *) Tloc(bn, BUNfirst(bn));
			}
			if (ri >= rhi)
				break;
			if (r->ttype == TYPE_void) {
				
#line 1837 "gdk_relop.mx"
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1901 "gdk_relop.mx"

			} else {
				
#line 1837 "gdk_relop.mx"
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1837 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1839 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1841 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1843 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1845 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1847 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1849 "gdk_relop.mx"

	ri++;
	
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1851 "gdk_relop.mx"

	ri++;


#line 1903 "gdk_relop.mx"

			}
		}
		/* do rest in more expensive loop */
		if (r->ttype == TYPE_void) {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1854 "gdk_relop.mx"
	{
		tdst[cur] = off++;
		cur += v == ra[ri];
	}


#line 1911 "gdk_relop.mx"

		} else {
			BUN cnt = BATcount(r);

			for (; ri < cnt; ri++)
				
#line 1859 "gdk_relop.mx"
	{
		tdst[cur] = rt[ri];
		cur += v == ra[ri];
	}


#line 1916 "gdk_relop.mx"

		}

		/* fill in the left oids for the generated result tuples */
 		o = * (oid *) BUNhead(lbi, li);
		while (len < cur)
			hdst[len++] = o;
	}
	BATsetcount(bn, cur);
	return 0;
}


#line 1938 "gdk_relop.mx"



#line 1945 "gdk_relop.mx"

static BAT *
BATnlthetajoin(BAT *l, BAT *r, int op, BUN estimate)
{
	int optimize = (l->htype == TYPE_oid || BAThdense(l)) && (r->ttype == TYPE_oid || BATtdense(r))
	    /* the follwoing might be trivial cases, but the
	     * "optimized" nlthetajoin implementation cannot handle
	     * them, yet ... */
	    && l->ttype != TYPE_void && r->htype != TYPE_void;
	BAT *bn = BATnew(ATOMtype(l->htype), ATOMtype(r->ttype), estimate >= BUN_MAX - 128 ? BUN_MAX : estimate + 128);
	int lo = 0, hi = 0;

	if (bn == NULL)
		return NULL;

	if (op == JOIN_GT) {
		lo = 1;
		hi = GDK_int_max;
		if (optimize)
			switch (ATOMstorage(l->ttype)) {
				
#line 1933 "gdk_relop.mx"
	
#line 1928 "gdk_relop.mx"
	case TYPE_bte:
		if (nlthetajoin_gt_bte(bn, l, r))
			goto bunins_failed;
		break;


#line 1933 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_sht:
		if (nlthetajoin_gt_sht(bn, l, r))
			goto bunins_failed;
		break;


#line 1934 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_int:
		if (nlthetajoin_gt_int(bn, l, r))
			goto bunins_failed;
		break;


#line 1935 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_lng:
		if (nlthetajoin_gt_lng(bn, l, r))
			goto bunins_failed;
		break;


#line 1936 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_flt:
		if (nlthetajoin_gt_flt(bn, l, r))
			goto bunins_failed;
		break;


#line 1937 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_dbl:
		if (nlthetajoin_gt_dbl(bn, l, r))
			goto bunins_failed;
		break;


#line 1938 "gdk_relop.mx"



#line 1965 "gdk_relop.mx"

			default:
				optimize = 0;
			}
	} else if (op == JOIN_GE) {
		lo = 0;
		hi = GDK_int_max;
		if (optimize)
			switch (ATOMstorage(l->ttype)) {
				
#line 1933 "gdk_relop.mx"
	
#line 1928 "gdk_relop.mx"
	case TYPE_bte:
		if (nlthetajoin_ge_bte(bn, l, r))
			goto bunins_failed;
		break;


#line 1933 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_sht:
		if (nlthetajoin_ge_sht(bn, l, r))
			goto bunins_failed;
		break;


#line 1934 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_int:
		if (nlthetajoin_ge_int(bn, l, r))
			goto bunins_failed;
		break;


#line 1935 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_lng:
		if (nlthetajoin_ge_lng(bn, l, r))
			goto bunins_failed;
		break;


#line 1936 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_flt:
		if (nlthetajoin_ge_flt(bn, l, r))
			goto bunins_failed;
		break;


#line 1937 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_dbl:
		if (nlthetajoin_ge_dbl(bn, l, r))
			goto bunins_failed;
		break;


#line 1938 "gdk_relop.mx"



#line 1974 "gdk_relop.mx"

			default:
				optimize = 0;
			}
	} else if (op == JOIN_LT) {
		lo = GDK_int_min;
		hi = -1;
		if (optimize)
			switch (ATOMstorage(l->ttype)) {
				
#line 1933 "gdk_relop.mx"
	
#line 1928 "gdk_relop.mx"
	case TYPE_bte:
		if (nlthetajoin_lt_bte(bn, l, r))
			goto bunins_failed;
		break;


#line 1933 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_sht:
		if (nlthetajoin_lt_sht(bn, l, r))
			goto bunins_failed;
		break;


#line 1934 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_int:
		if (nlthetajoin_lt_int(bn, l, r))
			goto bunins_failed;
		break;


#line 1935 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_lng:
		if (nlthetajoin_lt_lng(bn, l, r))
			goto bunins_failed;
		break;


#line 1936 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_flt:
		if (nlthetajoin_lt_flt(bn, l, r))
			goto bunins_failed;
		break;


#line 1937 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_dbl:
		if (nlthetajoin_lt_dbl(bn, l, r))
			goto bunins_failed;
		break;


#line 1938 "gdk_relop.mx"



#line 1983 "gdk_relop.mx"

			default:
				optimize = 0;
			}
	} else if (op == JOIN_LE) {
		lo = GDK_int_min;
		hi = 0;
		if (optimize)
			switch (ATOMstorage(l->ttype)) {
				
#line 1933 "gdk_relop.mx"
	
#line 1928 "gdk_relop.mx"
	case TYPE_bte:
		if (nlthetajoin_le_bte(bn, l, r))
			goto bunins_failed;
		break;


#line 1933 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_sht:
		if (nlthetajoin_le_sht(bn, l, r))
			goto bunins_failed;
		break;


#line 1934 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_int:
		if (nlthetajoin_le_int(bn, l, r))
			goto bunins_failed;
		break;


#line 1935 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_lng:
		if (nlthetajoin_le_lng(bn, l, r))
			goto bunins_failed;
		break;


#line 1936 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_flt:
		if (nlthetajoin_le_flt(bn, l, r))
			goto bunins_failed;
		break;


#line 1937 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_dbl:
		if (nlthetajoin_le_dbl(bn, l, r))
			goto bunins_failed;
		break;


#line 1938 "gdk_relop.mx"



#line 1992 "gdk_relop.mx"

			default:
				optimize = 0;
			}
	} else if (op == JOIN_EQ) {
		if (optimize)
			switch (ATOMstorage(l->ttype)) {
				
#line 1933 "gdk_relop.mx"
	
#line 1928 "gdk_relop.mx"
	case TYPE_bte:
		if (nlthetajoin_eq_bte(bn, l, r))
			goto bunins_failed;
		break;


#line 1933 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_sht:
		if (nlthetajoin_eq_sht(bn, l, r))
			goto bunins_failed;
		break;


#line 1934 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_int:
		if (nlthetajoin_eq_int(bn, l, r))
			goto bunins_failed;
		break;


#line 1935 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_lng:
		if (nlthetajoin_eq_lng(bn, l, r))
			goto bunins_failed;
		break;


#line 1936 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_flt:
		if (nlthetajoin_eq_flt(bn, l, r))
			goto bunins_failed;
		break;


#line 1937 "gdk_relop.mx"

	
#line 1928 "gdk_relop.mx"
	case TYPE_dbl:
		if (nlthetajoin_eq_dbl(bn, l, r))
			goto bunins_failed;
		break;


#line 1938 "gdk_relop.mx"



#line 1999 "gdk_relop.mx"

			default:
				optimize = 0;
			}

	}
	if (!optimize) {
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
		ptr nil = ATOMnilptr(l->ttype);
		BUN rp, rq, lp, lq;

		BATloop(l, lp, lq) {
			ptr v = (ptr) BUNtail(li, lp);

			if ((*cmp) (v, nil) == 0) {
				continue;
			}
			BATloop(r, rp, rq) {
				ptr w = (ptr) BUNhead(ri, rp);
				int c = (*cmp) (v, w);

				if ((c >= lo) & (c <= hi)) {
					bunfastins(bn, BUNhead(li, lp), BUNtail(ri, rp));
				}
			}
		}
	}
	bn->hsorted = l->hsorted || BATcount(bn) <= 1;
	bn->hrevsorted = l->hrevsorted || BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = r->T->nonil;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


/*
 * @+ Semijoin
 *
 * The BATsemijoin performs a semijoin over l and r. It returns
 * a subset of l that matches at least one element in r.
 * The result inherits the integrity properties.
 *
 * Various algorithms exist. The main one BATkintersect() resides
 * outside this file, in the set-operations implementation (gdk_setop).
 * Other variants for the semijoin include the fetch-semijoin
 * (for dense join columns), the reverse semijoin that loops over r
 * instead of l, and semijoin using binary search in r.
 */
#define semijoinbat(b, hs, ts, func)					\
	do {								\
		bn = BATnew(BAThtype(b), BATttype(b),			\
			    MAX(BATTINY, MIN(BATcount(l), BATcount(r)))); \
		ESTIDEBUG THRprintf(GDKout, "#%s.semijoinbat: estimated resultsize: " BUNFMT "\n", func, MAX(BATTINY, MIN(BATcount(l), BATcount(r)))); \
		if (bn == NULL)						\
			return bn;					\
		BATkey(bn, BAThkey(b));					\
		BATkey(BATmirror(bn), BATtkey(b));			\
		bn->hsorted = hs;					\
		bn->tsorted = ts;					\
	} while (0)


#line 2105 "gdk_relop.mx"
static BAT *
BATbinsemijoin(BAT *l, BAT *r, BAT *cpy)
{
	BAT *bn, *del = NULL;
	int loc, var;

	
#line 60 "gdk_relop.mx"
	ERRORcheck(l == NULL, "BATbinsemijoin: invalid left operand");
	ERRORcheck(r == NULL, "BATbinsemijoin: invalid right operand");
	ERRORcheck(TYPEerror(l->htype, r->htype), "BATbinsemijoin: type conflict\n");


#line 2111 "gdk_relop.mx"

	semijoinbat(cpy, TRUE, l == cpy && BATtordered(l), "BATbinsemijoin");

	if (!BAThordered(r)) {
		del = r = BATsort(r);
		if (del == NULL)
			goto bunins_failed;
	}

	switch (loc = var = ATOMstorage(l->htype)) {
	case TYPE_bte:
		
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, bte) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, bte)) {
				SORTloop_bte(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2122 "gdk_relop.mx"

	case TYPE_sht:
		
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, sht) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, sht)) {
				SORTloop_sht(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2124 "gdk_relop.mx"

	case TYPE_int:
		
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, int) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, int)) {
				SORTloop_int(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2126 "gdk_relop.mx"

	case TYPE_flt:
		
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, flt) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, flt)) {
				SORTloop_flt(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2128 "gdk_relop.mx"

	case TYPE_dbl:
		
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, dbl) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, dbl)) {
				SORTloop_dbl(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2130 "gdk_relop.mx"

	case TYPE_lng:
		
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, lng) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!simple_EQ(v, nil, lng)) {
				SORTloop_lng(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2132 "gdk_relop.mx"

	default:
		if (l->hvarsized) {
			if (r->hvarsized) {
				
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhvar(li, lp);

			if (!atom_EQ(v, nil, var) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhvar(li, lp);

			if (!atom_EQ(v, nil, var)) {
				SORTloop_var(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2136 "gdk_relop.mx"

			} else {
				
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhvar(li, lp);

			if (!atom_EQ(v, nil, loc) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhvar(li, lp);

			if (!atom_EQ(v, nil, loc)) {
				SORTloop_loc(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2138 "gdk_relop.mx"

			}
		} else {
			if (r->hvarsized) {
				
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!atom_EQ(v, nil, var) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!atom_EQ(v, nil, var)) {
				SORTloop_var(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2142 "gdk_relop.mx"

			} else {
				
#line 2075 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN lp, lq;
	ptr nil = ATOMnilptr(l->htype);

	if (cpy == l) {
		BATloop(l, lp, lq) {
			ptr v = BUNhloc(li, lp);

			if (!atom_EQ(v, nil, loc) && SORTfnd(r, v) != BUN_NONE) {
				bunfastins(bn, v, BUNtail(li, lp));
			}
		}
	} else {
		BATloop(l, lp, lq) {
			BUN rp, rq;
			ptr v = BUNhloc(li, lp);

			if (!atom_EQ(v, nil, loc)) {
				SORTloop_loc(BATmirror(r), rp, rq, v, v) {
					bunfastins(bn, v, BUNtail(ri, rp));
				}
			}
		}
	}
}
break;


#line 2144 "gdk_relop.mx"

			}
		}
	}

	/* propagate properties */
	bn->hsorted = l->hsorted || BATcount(bn) <= 1;
	bn->hrevsorted = l->hrevsorted || BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	if (BATcount(bn) == BATcount(l)) {
		if (l == cpy) {
			ALIGNset(bn, l);
		} else if (BAThkey(l) && BAThkey(r)) {
			ALIGNsetH(bn, l);
		}
	}
	bn->H->nonil = l->H->nonil & r->H->nonil;
	bn->T->nonil = l->T->nonil;
	if (del)
		BBPreclaim(del);
	ESTIDEBUG THRprintf(GDKout, "#BATbinsemijoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/*
 * The reverse semijoin is only better if the other side (r) is much
 * smaller than l, and iff you already have the hash table on l. It
 * uses hash tables on both relations: on r to check that no item is
 * processed twice (not necessary to check iff BAThkey(r) and one on l
 * to find the matching tuples.
 */


#line 2229 "gdk_relop.mx"
static BAT *
BATrevsemijoin(BAT *l, BAT *r)
{
	int any, rdoubles = (BAThkey(r) == 0), merge = rdoubles & BAThordered(r);
	BAT *bn;

	
#line 60 "gdk_relop.mx"
	ERRORcheck(l == NULL, "BATrevsemijoin: invalid left operand");
	ERRORcheck(r == NULL, "BATrevsemijoin: invalid right operand");
	ERRORcheck(TYPEerror(l->htype, r->htype), "BATrevsemijoin: type conflict\n");


#line 2235 "gdk_relop.mx"

	semijoinbat(l, FALSE, FALSE, "BATrevsemijoin");
	if (BATprepareHash(l))
		goto bunins_failed;
	if (rdoubles && BATprepareHash(r))
		goto bunins_failed;

	switch (any = ATOMstorage(l->htype)) {
	case TYPE_bte:
		
#line 2181 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN yy;
	BUN rp = 0, rq = 0;
	ptr nil = ATOMnilptr(l->htype);
	ptr v;

	if (merge) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: merge\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			yy = rp+1;
			if (yy < rq && simple_EQ(v, BUNhloc(ri, yy), bte))
				continue;
			if (!simple_EQ(v, nil, bte)) {
				HASHloop_bte(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else if (rdoubles) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: rdoubles\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			HASHloop_bte(ri, r->H->hash, yy, v)
				break;
			if (yy != rp)
				continue;
			if (!simple_EQ(v, nil, bte)) {
				HASHloop_bte(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else {
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);
			if (!simple_EQ(v, nil, bte)) {
				HASHloop_bte(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	}
}
break;


#line 2244 "gdk_relop.mx"

	case TYPE_sht:
		
#line 2181 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN yy;
	BUN rp = 0, rq = 0;
	ptr nil = ATOMnilptr(l->htype);
	ptr v;

	if (merge) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: merge\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			yy = rp+1;
			if (yy < rq && simple_EQ(v, BUNhloc(ri, yy), sht))
				continue;
			if (!simple_EQ(v, nil, sht)) {
				HASHloop_sht(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else if (rdoubles) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: rdoubles\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			HASHloop_sht(ri, r->H->hash, yy, v)
				break;
			if (yy != rp)
				continue;
			if (!simple_EQ(v, nil, sht)) {
				HASHloop_sht(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else {
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);
			if (!simple_EQ(v, nil, sht)) {
				HASHloop_sht(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	}
}
break;


#line 2246 "gdk_relop.mx"

	case TYPE_int:
	case TYPE_flt:
		
#line 2181 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN yy;
	BUN rp = 0, rq = 0;
	ptr nil = ATOMnilptr(l->htype);
	ptr v;

	if (merge) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: merge\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			yy = rp+1;
			if (yy < rq && simple_EQ(v, BUNhloc(ri, yy), int))
				continue;
			if (!simple_EQ(v, nil, int)) {
				HASHloop_int(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else if (rdoubles) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: rdoubles\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			HASHloop_int(ri, r->H->hash, yy, v)
				break;
			if (yy != rp)
				continue;
			if (!simple_EQ(v, nil, int)) {
				HASHloop_int(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else {
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);
			if (!simple_EQ(v, nil, int)) {
				HASHloop_int(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	}
}
break;


#line 2249 "gdk_relop.mx"

	case TYPE_dbl:
	case TYPE_lng:
		
#line 2181 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN yy;
	BUN rp = 0, rq = 0;
	ptr nil = ATOMnilptr(l->htype);
	ptr v;

	if (merge) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: merge\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			yy = rp+1;
			if (yy < rq && simple_EQ(v, BUNhloc(ri, yy), lng))
				continue;
			if (!simple_EQ(v, nil, lng)) {
				HASHloop_lng(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else if (rdoubles) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: rdoubles\n");
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);

			HASHloop_lng(ri, r->H->hash, yy, v)
				break;
			if (yy != rp)
				continue;
			if (!simple_EQ(v, nil, lng)) {
				HASHloop_lng(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else {
		BATloop(r, rp, rq) {
			v = BUNhloc(ri, rp);
			if (!simple_EQ(v, nil, lng)) {
				HASHloop_lng(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	}
}
break;


#line 2252 "gdk_relop.mx"

	case TYPE_str:
		if (r->H->vheap->hashash) {
			
#line 2181 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN yy;
	BUN rp = 0, rq = 0;
	ptr nil = ATOMnilptr(l->htype);
	ptr v;

	if (merge) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: merge\n");
		BATloop(r, rp, rq) {
			v = BUNhead(ri, rp);

			yy = rp+1;
			if (yy < rq && atom_EQ(v, BUNhead(ri, yy), any))
				continue;
			if (!atom_EQ(v, nil, any)) {
				HASHloop_str_hv(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else if (rdoubles) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: rdoubles\n");
		BATloop(r, rp, rq) {
			v = BUNhead(ri, rp);

			HASHloop_str_hv(ri, r->H->hash, yy, v)
				break;
			if (yy != rp)
				continue;
			if (!atom_EQ(v, nil, any)) {
				HASHloop_str_hv(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else {
		BATloop(r, rp, rq) {
			v = BUNhead(ri, rp);
			if (!atom_EQ(v, nil, any)) {
				HASHloop_str_hv(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	}
}
break;


#line 2255 "gdk_relop.mx"

		}
		/* fall through */
	default:
		
#line 2181 "gdk_relop.mx"
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN yy;
	BUN rp = 0, rq = 0;
	ptr nil = ATOMnilptr(l->htype);
	ptr v;

	if (merge) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: merge\n");
		BATloop(r, rp, rq) {
			v = BUNhead(ri, rp);

			yy = rp+1;
			if (yy < rq && atom_EQ(v, BUNhead(ri, yy), any))
				continue;
			if (!atom_EQ(v, nil, any)) {
				HASHloop_any(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else if (rdoubles) {
		ALGODEBUG fprintf(stderr, "#BATrevsemijoin: rdoubles\n");
		BATloop(r, rp, rq) {
			v = BUNhead(ri, rp);

			HASHloop_any(ri, r->H->hash, yy, v)
				break;
			if (yy != rp)
				continue;
			if (!atom_EQ(v, nil, any)) {
				HASHloop_any(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	} else {
		BATloop(r, rp, rq) {
			v = BUNhead(ri, rp);
			if (!atom_EQ(v, nil, any)) {
				HASHloop_any(li, l->H->hash, yy, v)
					bunfastins(bn, v, BUNtail(li, yy));
			}
		}
	}
}
break;


#line 2259 "gdk_relop.mx"

	}
	/* propagate properties */
	bn->hsorted = bn->hrevsorted = BATcount(bn) <= 1;
	bn->tsorted = bn->trevsorted = BATcount(bn) <= 1;
	if (BAThkey(r) && BATtkey(l) && BATcount(bn) == BATcount(r)) {
		ALIGNsetH(bn, r);
	}
	bn->H->nonil = l->H->nonil & r->H->nonil;
	bn->T->nonil = l->T->nonil;
	ESTIDEBUG THRprintf(GDKout, "#BATrevsemijoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/*
 * The positional semijoin performs a semijoin using positional
 * lookup.  This implementation is dirty as it also allows fetches
 * with hard integer positions, rather than oid matching on a
 * dense-oid column.
 */
static BAT *
BATfetchsemijoin(BAT *l, BAT *r, BAT *cpy, int denselookup)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BUN base, end, yy;
	ssize_t offset;
	BUN l_cur, l_end, r_cur;
	BAT *bn;

	BATcheck(l, "BATfetchsemijoin: left BAT required");
	BATcheck(r, "BATfetchsemijoin: right BAT required");

	if (denselookup) {
		if (!BAThdense(r)) {
			GDKerror("BATfetchsemijoin: left column must be dense.\n");
			return NULL;
		} else if (BATcount(l) && ATOMstorage(l->htype) != ATOMstorage(TYPE_oid)) {
			GDKerror("BATfetchsemijoin: illegal index type %s.\n", ATOMname(l->htype));
			return NULL;
		}
	}
	if (BATcount(l) && BAThvoid(l)) {
		/* redirect semijoin on two dense regions to a select
		 * (and hence to BATslice) */
		oid min = l->hseqbase, max = min;

		if (min != oid_nil)
			max += BATcount(l);
		if (denselookup) {
			min -= r->hseqbase;
			max -= r->hseqbase;
		}
		return BATslice(cpy, min, max);
	}
	base = BUNfirst(r);
	end = base + BATcount(r);
	bn = BATnew(BAThtype(cpy), BATttype(cpy), MIN(BATcount(r), BATcount(l)));
	if (bn == NULL)
		return bn;
	ESTIDEBUG THRprintf(GDKout, "#BATfetchsemijoin: estimated resultsize: " BUNFMT "\n", MIN(BATcount(r), BATcount(l)));

	if (bn == NULL) {
		return NULL;
	}
	if (denselookup) {
		offset = (ssize_t) base - (ssize_t) r->hseqbase;	/* translate oid to BUN position */
	} else {
		offset = (ssize_t) base;	/* fetch by hard BUNindex */
	}

	/* iterate l; positional fetch in r */
	BATloop(l, l_cur, l_end) {
		yy = (BUN) (offset + *(oid *) BUNhloc(li, l_cur));
		if (yy < base || yy >= end) {
			continue;
		}
		r_cur = yy;
		if (cpy == r) {
			bunfastins(bn, BUNhead(ri, r_cur), BUNtail(ri, r_cur));
		} else {
			bunfastins(bn, BUNhead(ri, r_cur), BUNtail(li, l_cur));
		}
	}

	/* property propagation */
	bn->hsorted = BATcount(bn) <= 1 ||
		BAThordered(l) & BAThordered(r) ||
		BAThrevordered(l) & BAThrevordered(r);
	bn->hrevsorted = BATcount(bn) <= 1 ||
		BAThordered(l) & BAThrevordered(r) ||
		BAThrevordered(l) & BAThordered(r);
	if (cpy == r) {
		bn->tsorted = BATcount(bn) <= 1 ||
			BAThordered(l) & BATtordered(r) ||
			BAThrevordered(l) & BATtrevordered(r);
		bn->trevsorted = BATcount(bn) <= 1 ||
			BAThordered(l) & BATtrevordered(r) ||
			BAThrevordered(l) & BATtordered(r);
	} else {
		bn->tsorted = BATcount(bn) <= 1 || BATtordered(l);
		bn->trevsorted = BATcount(bn) <= 1 || BATtrevordered(l);
	}
	bn->H->nonil = l->H->nonil & r->H->nonil;
	bn->T->nonil = cpy->T->nonil;

	if (denselookup && BATcount(bn) == BATcount(l)) {
		ALIGNsetH(bn, l);
	} else {
		BATkey(bn, BAThkey(l) && BAThkey(r));
	}
	if (BAThkey(l)) {
		if (BATcount(bn) == BATcount(cpy) && (BAThordered(r) & BAThordered(l))) {
			ALIGNsetT(bn, cpy);
		} else {
			BATkey(BATmirror(bn), BATtkey(cpy));
		}
	}
	ESTIDEBUG THRprintf(GDKout, "#BATfetchsemijoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATfetch(BAT *l, BAT *r)
{
	return BATfetchsemijoin(r, l, l, FALSE);
}

/*
 * The BATsemijoin chooses between various alternatives.
 */

BAT *
BATsemijoin(BAT *l, BAT *r)
{
	int reverse1, reverse2;
	BUN countr, countl, i;
	lng logr, logl;
	BAT *bn, *tmp = NULL;

	ERRORcheck(l == NULL, "BATsemijoin");
	ERRORcheck(r == NULL, "BATsemijoin");
	ERRORcheck(TYPEerror(l->htype, r->htype), "BATsemijoin: type conflict\n");

	/*
	 * @- algorithm selection
	 * We have 10 algorithms implementing semijoin. Their
	 * conditions are checked in order of efficiency. Some
	 * algorithms reverse the semijoin (loop over r, lookup in l).
	 * To do that r should be unique. To that end, doubles may
	 * sometimes be eliminated from r.
	 */
	for (logr = 4, i = countr = BATcount(r); i > 0; logr++)
		i >>= 1;
	for (logl = 4, i = countl = BATcount(l); i > 0; logl++)
		i >>= 1;
	reverse1 = countr < countl && (BAThkey(r) || (lng) countr * 8 < (lng) countl);
	reverse2 = (lng) countr *logl < (lng) countl && (BAThkey(r)
							 || (lng) countr * (logl + 8) < (lng) countl);

	if (ALIGNsynced(l, r)) {
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATcopy(l);\n");

		bn = BATcopy(l, l->htype, l->ttype, FALSE);
	} else if (BAThordered(l) && BAThdense(r)) {
		oid lo = r->hseqbase;
		oid hi = r->hseqbase + countr - 1;
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATmirror(BATselect(BATmirror(l), &lo, &hi));\n");

		bn = BATmirror(BATselect(BATmirror(l), &lo, &hi));
	} else if (BAThdense(r)) {
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATfetchsemijoin(l, r, l);\n");

		bn = BATfetchsemijoin(l, r, l, TRUE);
	} else if (BAThdense(l) && reverse1) {
		if (!BAThkey(r)) {
			BAT *v = VIEWhead_(r, BAT_WRITE);

			tmp = r = BATkunique(v);
			BBPreclaim(v);
		}
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATfetchsemijoin(r, l, l);\n");

		bn = BATfetchsemijoin(r, l, l, TRUE);
	} else if (l->H->hash && reverse1) {
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATrevsemijoin(l,r);\n");

		bn = BATrevsemijoin(l, r);
	} else if (BAThordered(r) && countl * logr < countr) {
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATbinsemijoin(l, r, l);\n");

		bn = BATbinsemijoin(l, r, l);
	} else if (BAThordered(l) & reverse2) {
		if (!BAThkey(r)) {
			BAT *v = VIEWhead_(r, BAT_WRITE);

			tmp = r = BATkunique(v);
			BBPreclaim(v);
		}
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATbinsemijoin(r, l, l);\n");

		bn = BATbinsemijoin(r, l, l);
	} else {
		ALGODEBUG fprintf(stderr, "#BATsemijoin: BATkintersect(l, r);\n");

		bn = BATkintersect(l, r);	/* merge-semijoin or nested hashlookup in r */
	}
	if (tmp) {
		BBPreclaim(tmp);
	}
	/* invest in property check, since we cannot easily derive the
	 * result properties, but later operations might benefit from
	 * / depend on them
	 * Disable via command line option --debug=16777216
	 */
	JOINPROPCHK {
		if (bn)
			BATderiveHeadProps(BATmirror(bn), 0);
	}
	return bn;
}

/*
 * @+ AntiJoin
 * This operation computes the cross product of two BATs, returning only the
 * head-value from the 'left' operand and then tail-value from the 'right'
 * provided the tail-head pair do not (!) match.
 */


#line 2546 "gdk_relop.mx"

#line 2538 "gdk_relop.mx"
	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_bte_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_bte_bte();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2538 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_bte_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_bte_sht();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2539 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_bte_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_bte_int();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2540 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_bte_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_bte_lng();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2541 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_bte_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_bte_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2542 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_bte_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_bte_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2543 "gdk_relop.mx"



#line 2546 "gdk_relop.mx"


#line 2538 "gdk_relop.mx"
	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_sht_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_sht_bte();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2538 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_sht_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_sht_sht();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2539 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_sht_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_sht_int();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2540 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_sht_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_sht_lng();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2541 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_sht_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_sht_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2542 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_sht_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_sht_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2543 "gdk_relop.mx"



#line 2547 "gdk_relop.mx"


#line 2538 "gdk_relop.mx"
	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_int_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_int_bte();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2538 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_int_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_int_sht();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2539 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_int_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_int_int();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2540 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_int_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_int_lng();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2541 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_int_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_int_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2542 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_int_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_int_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2543 "gdk_relop.mx"



#line 2548 "gdk_relop.mx"


#line 2538 "gdk_relop.mx"
	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_lng_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_lng_bte();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2538 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_lng_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_lng_sht();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2539 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_lng_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_lng_int();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2540 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_lng_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_lng_lng();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2541 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_lng_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_lng_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2542 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_lng_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_lng_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2543 "gdk_relop.mx"



#line 2549 "gdk_relop.mx"



#line 2538 "gdk_relop.mx"
	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_VATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_VATOM_bte();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2538 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_VATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_VATOM_sht();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2539 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_VATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_VATOM_int();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2540 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_VATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_VATOM_lng();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2541 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_VATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_VATOM_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2542 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_VATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_VATOM_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2543 "gdk_relop.mx"



#line 2551 "gdk_relop.mx"


#line 2538 "gdk_relop.mx"
	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_LATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_LATOM_bte();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2538 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_LATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_LATOM_sht();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2539 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_LATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_LATOM_int();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2540 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_LATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_LATOM_lng();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2541 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_LATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_LATOM_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2542 "gdk_relop.mx"

	
#line 2496 "gdk_relop.mx"
static BAT *
antijoin_LATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;
	int (*cmp) (const void *, const void *) = BATatoms[l->ttype].atomCmp;
	ptr nil = ATOMnilptr(l->ttype);

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATantijoin: antijoin_LATOM_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		ptr v = (ptr) BUNtail(li, l_cur);
		BATloop(r, r_cur, r_end) {
			ptr w = (ptr) BUNhead(ri, r_cur);
			int c = (*cmp)(v, w);
			if ((*cmp)(v, nil) != 0 && (*cmp)(w, nil) != 0 && c != 0 ) {
				
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2524 "gdk_relop.mx"

				dst++;
			}
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2543 "gdk_relop.mx"



#line 2552 "gdk_relop.mx"




#line 2583 "gdk_relop.mx"


#line 2612 "gdk_relop.mx"
BAT *
BATantijoin(BAT *l, BAT *r)
{
	BAT *bn;
	BUN lc, rc, sz;

	ERRORcheck(l == NULL, "BATantijoin: invalid left operand");
	ERRORcheck(r == NULL, "BATantijoin: invalid right operand");
	lc = BATcount(l);
	rc = BATcount(r);
	sz = (BUN) MIN((lng) lc * rc, BUN_MAX);

	assert(sz <= BUN_MAX);
	if (sz > 0) {
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);

		if (rc == 1) {
			l = BATantiuselect_(l, BUNhead(ri, BUNfirst(r)), NULL, 1, 1);
			bn = BATconst(l, BATttype(r), BUNtail(ri, BUNfirst(r)));
			BBPunfix(l->batCacheid);
			return bn;
		}
		if (lc == 1) {
			r = BATantiuselect_(BATmirror(r), BUNtail(li, BUNfirst(l)), NULL, 1, 1);
			bn = BATmirror(BATconst(r, BAThtype(l), BUNhead(li, BUNfirst(l))));
			BBPunfix(r->batCacheid);
			return bn;
		}
	}

	bn = BATnew(BAThtype(l), BATttype(r), sz);
	if (bn == NULL)
		return bn;
	if (sz == 0)
		return bn;

	
#line 2584 "gdk_relop.mx"
{
	int lht = l->htype;
	int lhs = ATOMstorage(lht);

	if (lhs == TYPE_bte) {
		
#line 2555 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = antijoin_bte_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = antijoin_bte_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = antijoin_bte_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = antijoin_bte_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = antijoin_bte_VATOM(bn, l, r);
	} else {
		bn = antijoin_bte_LATOM(bn, l, r);
	}
}


#line 2589 "gdk_relop.mx"

	} else if (lhs == TYPE_sht) {
		
#line 2555 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = antijoin_sht_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = antijoin_sht_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = antijoin_sht_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = antijoin_sht_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = antijoin_sht_VATOM(bn, l, r);
	} else {
		bn = antijoin_sht_LATOM(bn, l, r);
	}
}


#line 2591 "gdk_relop.mx"

	} else if (lhs == TYPE_int || lhs == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || lhs == TYPE_oid
#endif
		) {
		
#line 2555 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = antijoin_int_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = antijoin_int_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = antijoin_int_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = antijoin_int_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = antijoin_int_VATOM(bn, l, r);
	} else {
		bn = antijoin_int_LATOM(bn, l, r);
	}
}


#line 2597 "gdk_relop.mx"

	} else if (lhs == TYPE_lng || lhs == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || lhs == TYPE_oid
#endif
		   ) {
		
#line 2555 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = antijoin_lng_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = antijoin_lng_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = antijoin_lng_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = antijoin_lng_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = antijoin_lng_VATOM(bn, l, r);
	} else {
		bn = antijoin_lng_LATOM(bn, l, r);
	}
}


#line 2603 "gdk_relop.mx"

	} else if (l->hvarsized) {
		
#line 2555 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = antijoin_VATOM_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = antijoin_VATOM_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = antijoin_VATOM_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = antijoin_VATOM_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = antijoin_VATOM_VATOM(bn, l, r);
	} else {
		bn = antijoin_VATOM_LATOM(bn, l, r);
	}
}


#line 2605 "gdk_relop.mx"

	} else {
		
#line 2555 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = antijoin_LATOM_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = antijoin_LATOM_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = antijoin_LATOM_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = antijoin_LATOM_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = antijoin_LATOM_VATOM(bn, l, r);
	} else {
		bn = antijoin_LATOM_LATOM(bn, l, r);
	}
}


#line 2607 "gdk_relop.mx"

	}
}


#line 2649 "gdk_relop.mx"


	if (bn) {
		bn->hsorted = l->hsorted || BATcount(bn) <= 1;
		bn->hrevsorted = l->hrevsorted || BATcount(bn) <= 1;
		bn->tsorted = (lc == 1 && r->tsorted) || BATcount(bn) <= 1;
		bn->trevsorted = (lc == 1 && r->trevsorted) || BATcount(bn) <= 1;
		bn->hdense = rc == 1 && l->hdense;
		bn->tdense = lc == 1 && r->tdense;
		BATkey(bn, rc == 1 && BAThkey(l));
		BATkey(BATmirror(bn), lc == 1 && BATtkey(r));
		bn->H->nonil = l->H->nonil;
		bn->T->nonil = r->T->nonil;
		if (!bn->batDirty)
			bn->batDirty = TRUE;
	}

	return bn;
}

/*
 * @+ Cross Product
 * This operation computes the cross product of two BATs, returning only the
 * head-value from the 'left' operand and then tail-value from the 'right'
 * operand.
 */


#line 2719 "gdk_relop.mx"

#line 2711 "gdk_relop.mx"
	
#line 2676 "gdk_relop.mx"
static BAT *
cross_bte_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_bte_bte();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2711 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_bte_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_bte_sht();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2712 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_bte_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_bte_int();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2713 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_bte_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_bte_lng();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2714 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_bte_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_bte_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2715 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_bte_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_bte_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hbteput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2716 "gdk_relop.mx"



#line 2719 "gdk_relop.mx"


#line 2711 "gdk_relop.mx"
	
#line 2676 "gdk_relop.mx"
static BAT *
cross_sht_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_sht_bte();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2711 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_sht_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_sht_sht();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2712 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_sht_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_sht_int();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2713 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_sht_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_sht_lng();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2714 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_sht_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_sht_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2715 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_sht_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_sht_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hshtput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2716 "gdk_relop.mx"



#line 2720 "gdk_relop.mx"


#line 2711 "gdk_relop.mx"
	
#line 2676 "gdk_relop.mx"
static BAT *
cross_int_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_int_bte();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2711 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_int_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_int_sht();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2712 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_int_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_int_int();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2713 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_int_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_int_lng();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2714 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_int_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_int_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2715 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_int_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_int_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hintput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2716 "gdk_relop.mx"



#line 2721 "gdk_relop.mx"


#line 2711 "gdk_relop.mx"
	
#line 2676 "gdk_relop.mx"
static BAT *
cross_lng_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_lng_bte();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2711 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_lng_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_lng_sht();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2712 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_lng_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_lng_int();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2713 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_lng_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_lng_lng();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2714 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_lng_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_lng_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2715 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_lng_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_lng_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	Hlngput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2716 "gdk_relop.mx"



#line 2722 "gdk_relop.mx"



#line 2711 "gdk_relop.mx"
	
#line 2676 "gdk_relop.mx"
static BAT *
cross_VATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_VATOM_bte();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2711 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_VATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_VATOM_sht();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2712 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_VATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_VATOM_int();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2713 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_VATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_VATOM_lng();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2714 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_VATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_VATOM_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2715 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_VATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_VATOM_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HVATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2716 "gdk_relop.mx"



#line 2724 "gdk_relop.mx"


#line 2711 "gdk_relop.mx"
	
#line 2676 "gdk_relop.mx"
static BAT *
cross_LATOM_bte(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_LATOM_bte();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tbteput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2711 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_LATOM_sht(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_LATOM_sht();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tshtput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2712 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_LATOM_int(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_LATOM_int();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tintput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2713 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_LATOM_lng(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_LATOM_lng();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	Tlngput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2714 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_LATOM_VATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_LATOM_VATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TVATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2715 "gdk_relop.mx"

	
#line 2676 "gdk_relop.mx"
static BAT *
cross_LATOM_LATOM(BAT *bn, BAT *l, BAT *r)
{
	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	BATiter bni;
	BUN l_cur, l_end, r_cur, r_end, dst;

	/* Just to silence compilers (Intel's icc) that otherwise might
	 * complain about "declared but never referenced" labels
	 * (condition should never be true).
	 * (A "dead" goto between the return and the label makes (other)
	 * compilers (Sun) complain about never reached code...)
	 */
	if (!bn)
		goto bunins_failed;

	bni = bat_iterator(bn);
	dst = BUNfirst(bn);
	ALGODEBUG fprintf(stderr, "#BATcross: cross_LATOM_LATOM();\n");
	BATloop(l, l_cur, l_end) {
		BATloop(r, r_cur, r_end) {
			
#line 568 "gdk_relop.mx"
	HLATOMput(bn, BUNhloc(bni, dst));
	TLATOMput(bn, BUNtloc(bni, dst));


#line 2698 "gdk_relop.mx"

			dst++;
		}
	}
	BATsetcount(bn, dst);

	return bn;

bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


#line 2716 "gdk_relop.mx"



#line 2725 "gdk_relop.mx"




#line 2756 "gdk_relop.mx"


#line 2785 "gdk_relop.mx"
BAT *
BATcross(BAT *l, BAT *r)
{
	BAT *bn;
	BUN lc, rc, sz;

	ERRORcheck(l == NULL, "BATcross: invalid left operand");
	ERRORcheck(r == NULL, "BATcross: invalid right operand");
	lc = BATcount(l);
	rc = BATcount(r);
	sz = (BUN) MIN((lng) lc * rc, BUN_MAX);
	assert(sz <= BUN_MAX);

	if (sz > 0) {
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);

		/* try to keep void columns where possible */
		if (rc == 1)
			return BATconst(l, BATttype(r), BUNtail(ri, BUNfirst(r)));
		if (lc == 1)
			return BATmirror(BATconst(BATmirror(r), BAThtype(l), BUNhead(li, BUNfirst(l))));
	}

	bn = BATnew(BAThtype(l), BATttype(r), sz);
	if (bn == NULL)
		return bn;
	if (sz == 0)
		return bn;

	
#line 2757 "gdk_relop.mx"
{
	int lht = l->htype;
	int lhs = ATOMstorage(lht);

	if (lhs == TYPE_bte) {
		
#line 2728 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = cross_bte_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = cross_bte_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = cross_bte_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = cross_bte_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = cross_bte_VATOM(bn, l, r);
	} else {
		bn = cross_bte_LATOM(bn, l, r);
	}
}


#line 2762 "gdk_relop.mx"

	} else if (lhs == TYPE_sht) {
		
#line 2728 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = cross_sht_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = cross_sht_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = cross_sht_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = cross_sht_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = cross_sht_VATOM(bn, l, r);
	} else {
		bn = cross_sht_LATOM(bn, l, r);
	}
}


#line 2764 "gdk_relop.mx"

	} else if (lhs == TYPE_int || lhs == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || lhs == TYPE_oid
#endif
		) {
		
#line 2728 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = cross_int_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = cross_int_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = cross_int_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = cross_int_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = cross_int_VATOM(bn, l, r);
	} else {
		bn = cross_int_LATOM(bn, l, r);
	}
}


#line 2770 "gdk_relop.mx"

	} else if (lhs == TYPE_lng || lhs == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || lhs == TYPE_oid
#endif
		   ) {
		
#line 2728 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = cross_lng_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = cross_lng_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = cross_lng_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = cross_lng_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = cross_lng_VATOM(bn, l, r);
	} else {
		bn = cross_lng_LATOM(bn, l, r);
	}
}


#line 2776 "gdk_relop.mx"

	} else if (l->hvarsized) {
		
#line 2728 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = cross_VATOM_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = cross_VATOM_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = cross_VATOM_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = cross_VATOM_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = cross_VATOM_VATOM(bn, l, r);
	} else {
		bn = cross_VATOM_LATOM(bn, l, r);
	}
}


#line 2778 "gdk_relop.mx"

	} else {
		
#line 2728 "gdk_relop.mx"
{
	int rtt = r->ttype;
	int rts = ATOMstorage(rtt);

	if (rts == TYPE_bte) {
		bn = cross_LATOM_bte(bn, l, r);
	} else if (rts == TYPE_sht) {
		bn = cross_LATOM_sht(bn, l, r);
	} else if (rts == TYPE_int || rts == TYPE_flt
#if SIZEOF_OID == SIZEOF_INT
		   || rts == TYPE_oid
#endif
		) {
		bn = cross_LATOM_int(bn, l, r);
	} else if (rts == TYPE_lng || rts == TYPE_dbl
#if SIZEOF_OID == SIZEOF_LNG
		   || ATOMstorage(lht) == TYPE_oid
#endif
) {
		bn = cross_LATOM_lng(bn, l, r);
	} else if (r->tvarsized) {
		bn = cross_LATOM_VATOM(bn, l, r);
	} else {
		bn = cross_LATOM_LATOM(bn, l, r);
	}
}


#line 2780 "gdk_relop.mx"

	}
}


#line 2815 "gdk_relop.mx"


	if (bn) {
		bn->hsorted = l->hsorted;
		bn->hrevsorted = l->hrevsorted;
		bn->tsorted = lc == 1 && r->tsorted;
		bn->trevsorted = lc == 1 && r->trevsorted;
		bn->hdense = rc == 1 && l->hdense;
		bn->tdense = lc == 1 && r->tdense;
		BATkey(bn, rc == 1 && BAThkey(l));
		BATkey(BATmirror(bn), lc == 1 && BATtkey(r));
		if (!bn->batDirty)
			bn->batDirty = TRUE;
		bn->H->nonil = l->H->nonil;
		bn->T->nonil = r->T->nonil;
	}

	return bn;
}

/*
 * @+ Cartesian product
 * The matching algorithms tries to construct non-empty matches on all head
 * columns. Each time this succeeds, it calls the Cartesian routine to
 * construct a join result that consists of the Cartesian product of these
 * matches.
 *
 * The matching tuples can be encoded in two ways:
 * @table @samp
 * @item clustered
 *  here we have two BUN pointers 'hi' and 'lo' that point
 * to a consecutive range of BUNs in a BAT that match.
 * @item nonclustered here we have a hit pointer that points to an array
 * of BUN  pointers that match.
 * @end table
 * The below structures are used for keeping track of the matching process.
 */
typedef struct _column_t {
	BAT *b;			/* BAT of this column */
	BATiter bi;
	BUN cur;		/* current BUN in b */
	BUN nhits;		/* number of matched BUNs */

	/* clustered */
	BUN lo;			/* first BUN that matches */
	BUN hi;			/* past last BUN that matches */
	/* nonclustered */
	BUN *hit;		/* BUN array pointer */
	size_t hitsize;		/* size of hit array */

	/* properties */
/* I'm not sure whether offset can become negative, so to be on the
 * save side, use a signed type.  However the magnitude should be
 * within the range allowed by BUN, so the casts associated with this
 * value should be OK. */
	ssize_t offset;		/* BUNindex of BUNfirst  */
	struct _column_t *sync;	/* iff > 0: column with synchronous BAT */
	BUN size;		/* size of the BAT */
	char binsearch;		/* sparse matching expected? */
	char ordered;		/* merge matching */
} column_t;

typedef struct {
	RowFcn tuple_fcn;	/* function to invoke per match */
	ptr tuple_data;		/* application-specific data */
	ColFcn *value_fcn;	/* for each col: function to invoke per value */
	ptr *value_data;	/* for each col: application-specific data */
	column_t *c;		/* array of columns */
	int argc;		/* size of c */
} table_t;

static void
column_result(table_t *t, int i)
{
	if (++i > t->argc) {
		/* end of recursion: invoke tuple-match routine */
		t->tuple_fcn(t->tuple_data, t->value_data);
	} else {
		/* recurse over all matches on this column */
		column_t *c = t->c + (i - 1);
		BUN q, *p = c->hit;
		BUN j;

		if (p == NULL) {	/* clustered */
			for (q = c->lo; q < c->hi; q++) {
				t->value_fcn[i] (t->value_data[i], BUNtail(c->bi, q));
				column_result(t, i);
			}
		} else {
			for (j = 0; j < c->nhits; j++, p++) {
				t->value_fcn[i] (t->value_data[i], BUNtail(c->bi, *p));
				column_result(t, i);
			}
		}
	}
}

/*
 * @* MultiColumn Joins
 * Computes the n-ary equijoin over the head columns of multiple BATs.
 * This function is complex, and uses nested functions calls,
 * for the specific stuff, it uses the stack for generating the
 * Cartesian product on each hit tuple. Most of all, it emits tuples one
 * at a time, in a pipeline rather than bulk fashion. For all these reasons,
 * it is not main-memory efficient. It does things that MonetDB actually
 * specifically was designed to avoid.
 *
 * USE THIS FUNCTION ONLY WHEN YOU REALLY REALLY HAVE TO:
 * @table @code
 * @item  -
 * printing a multicolumn table to a watching end-user is one such example
 * @end table
 * @+ multijoin entry routine
 * The multijoin will cause a cascade of value_fcn() calls putting
 * values in to place, rounded off each time by a tuple_fcn() that is
 * executed on each produced tuple. If this corresponds 1-1 with
 * the elements of one of the parameter BAT, the 'result' of the
 * operation would be aligned with it.
 *
 * The return value of this operation contains this status information.
 * It is an integer, of which all 4 bytes are used:
 * @table @code
 * @item ret[0] == 1,
 * if a mergejoin was used, 0 otherwise
 * @item ret[1] == 1,
 * if all bats had the key property set, 0 otherwise
 * @item ret[2] == 1
 * if there was a 1-1 join, 0 otherwise
 * @item ret[3] ==
 * the parameter number of the BAT which was used as leader
 * @end table
 */
#define COLSIZE(c)\
	(((c)->b->htype!=TYPE_void || (c)->b->hseqbase!=oid_nil)?(c)->size:0)
#define REALLOCBUNS(c,n) if (c->hitsize <= n)\
	c->hit = (BUN*) GDKrealloc(c->hit, (c->hitsize+=n)*sizeof(BUN))

#define LEAD_INTERRUPT_END  1
#define LEAD_TRAVERSE_SSK   3	/* seq, sorted, key */
#define LEAD_TRAVERSE_SNK   4	/* seq, nonsorted, key */
#define LEAD_TRAVERSE_SEQ   6	/* enforced seq (for order purposes) */
#define LEAD_TRAVERSE_SRT   9	/* traverse by sorted chunk */

int
BATmultijoin(int argc, BAT *argv[], RowFcn tuple_fcn, ptr tuple_data, ColFcn value_fcn[], ptr value_data[], int orderby)
{
	column_t *lead_col, *c = (column_t *) GDKzalloc(argc * (int) sizeof(column_t));
	column_t **reorder = (column_t **) GDKmalloc(argc * (int) sizeof(column_t *));
	int status = 0, algo = LEAD_TRAVERSE_SEQ;
	int i, k;
	BUN p, q;
	table_t t;

	/*
	 * Init the table descriptor.
	 */
	if (c == NULL || reorder == NULL) {
		GDKfree(c);
		GDKfree(reorder);
		return 0;
	}
	t.tuple_data = tuple_data;
	t.value_data = value_data;
	t.tuple_fcn = tuple_fcn;
	t.value_fcn = value_fcn;
	t.argc = argc;
	t.c = c;
	/*
	 * order columns by their size (smallest first)
	 */
	for (i = 0; i < argc; i++) {
		int j;

		c[i].b = argv[i];
		c[i].bi = bat_iterator(c[i].b);
		c[i].nhits = 1;	/* default value */
		c[i].offset = (ssize_t) BUNfirst(c[i].b);
		c[i].size = BATcount(c[i].b);

		/* positional lookup possible => ignore other alternatives */
		if (!BAThdense(c[i].b))
			c[i].ordered = BAThordered(c[i].b);

		/* insertion sort on size */
		for (j = 0; j < i; j++) {
			if (COLSIZE(reorder[j]) > COLSIZE(c + i) ||
			    /* in case of equal size, we prefer dense over non-dense */
			    (COLSIZE(reorder[j]) == COLSIZE(c + i) && !BAThdense(reorder[j]->b) && BAThdense((c + i)->b))) {
				for (k = i; k > j; k--) {
					reorder[k] = reorder[k - 1];
				}
				break;
			}
		}
		reorder[j] = c + i;
	}
	/*
	 * @- handle explicit ordering requests
	 * An 'orderby' specification tells that the multijoin should
	 * match in the order of one specific BAT parameter.
	 *
	 * Notice that we *respect* the ordering of the orderby column
	 * rather than we sort it explicitly (ie; you should order the
	 * most significant column beforehand).  This allows for both
	 * for join results ordered on some tail column as results
	 * ordered on head column, or even 'reverse' or other specific
	 * orderings.  One such specific ordering is the SQL ORDER BY
	 * multi-column ordering that can be obtained with the
	 * CTorderby command from the xtables module.
	 */
	if (orderby) {		/* order on tail of some column */
		int lead = orderby - 1;

		for (i = 0; i < argc; i++)
			if (reorder[i] == c + lead)
				break;
		while (--i >= 0) {
			reorder[i + 1] = reorder[i];
		}
		reorder[0] = c + lead;
	}
	lead_col = reorder[0];
	/*
	 * @- lead column traversal mode
	 * The default action is to do LEAD_TRAVERSE_SEQ: 1-by-1
	 * traversal of the lead_col, and for each head value use the
	 * best possible matching algorithm.  A local optimization is
	 * to signal a sorted head column in the lead column, so we
	 * can switch to LEAD_TRAVERSE_SRT; if double lead values
	 * occur we do them in one match iteration.
	 *
	 * We record in MULTIJOIN_SORTED(status) whether the chosen
	 * traversal method visits the head values in the lead column
	 * in order. This is important for the matching algorithms of
	 * the other columns (only if the head values are visited in
	 * order, merge algorithms may be employed).
	 */
	if (BAThordered(lead_col->b)) {
		if (!BAThkey(lead_col->b)) {
			algo = LEAD_TRAVERSE_SRT;
		}
		MULTIJOIN_SORTED(status) = TRUE;
	}
	lead_col->hi = lead_col->cur = BUNfirst(lead_col->b);
	q = BUNlast(lead_col->b);
	MULTIJOIN_KEY(status) = (char) BAThkey(lead_col->b);
	MULTIJOIN_LEAD(status) = (char) (lead_col - c);
	MULTIJOIN_SYNCED(status) = 1;
	if (algo == LEAD_TRAVERSE_SEQ && BAThkey(lead_col->b)) {
		algo = BAThordered(lead_col->b) ? LEAD_TRAVERSE_SSK : LEAD_TRAVERSE_SNK;
	}

	/*
	 * @- matching algorithms for the other columns
	 * Finally, the issue of choosing matching-algorithms for the
	 * other columns is treated. There are a number of
	 * possibilities. If a column is synced with a previous
	 * column, this is registered, so it can copy the matching
	 * results of that previous column. If not, we use the fact
	 * that a column is ordered and if not, has an binary index on
	 * it. Both cases fall into two sub cases: merge-lookup or
	 * binary-search; depending on whether we visit the head
	 * elements in order (MULTIJOIN_SORTED(status)).  If none of
	 * this is the case, we do hash-lookup using an on-the-fly
	 * hash-table.
	 */
	for (k = 1; k < argc; k++) {
		column_t *n = reorder[k];
		int j;

		for (j = (algo == LEAD_TRAVERSE_SEQ); j < k; j++) {
			if (ALIGNsynced(reorder[j]->b, n->b)) {
				n->sync = (struct _column_t *) reorder[j];
				n->offset -= reorder[j]->offset;
			}
		}
		if (!BAThkey(n->b)) {
			MULTIJOIN_KEY(status) = 0;
			MULTIJOIN_SYNCED(status) = 0;
		}
		if (!MULTIJOIN_SORTED(status)) {
			if (n->size < 4 * lead_col->size) {
				n->ordered = FALSE;
			} else {
				n->binsearch = TRUE;
			}
		} else if (n->size > 40 * lead_col->size) {
			n->binsearch = TRUE;
		}
		if (n->ordered) {
			n->cur = BUNfirst(n->b);
		} else if (!BAThkey(n->b) && n->sync == NULL) {
			if (BATprepareHash(n->b)) {
				GDKerror("BATmultijoin: could not hash '%s'\n", BATgetId(n->b));
				GDKfree(c);
				GDKfree(reorder);
				return 0;
			}
			n->hitsize = 20;
			n->hit = (BUN *) GDKmalloc(n->hitsize * sizeof(BUN));
			if (n->hit == NULL) {
				GDKfree(c);
				GDKfree(reorder);
				return 0;
			}
		}
	}

/*
 * @- the matching phase
 * We optimize in the case that the head-columns are OID. Below
 * macro's help to separate the two cases cleanly.
 */
#if (SIZEOF_OID == SIZEOF_INT)
#define OIDcmp(v1,v2)	simple_CMP(v1,v2,int)
#else
#define OIDcmp(v1,v2)	simple_CMP(v1,v2,lng)
#endif
#define STDcmp(v1,v2)	(*cmp)(v1,v2)

	if (ATOMstorage(lead_col->b->htype) == ATOMstorage(TYPE_oid)) {
		
#line 3210 "gdk_relop.mx"
	while (algo) {
		ptr h;		/* points to current ID */

		/*
		 * find the next leader bun
		 */
		p = lead_col->hi;
		if (p >= q)
			break;
		h = BUNhead(lead_col->bi, p);

		/* FIND MATCHING COLUMN RANGES
		 * For each column, find all matches for this head value
		 */
		for (i = 0; i < argc; i++) {
			column_t *m, *n = reorder[i];	/* use BATcount() order */
			BAT *b = n->b;
			BATiter bi = n->bi;

			/* one-by-one traversal of the lead column? => no matching done.
			 */
			if (n == lead_col) {
				if (argc > 1 && ATOMcmp(b->htype, h, ATOMnilptr(b->htype)) == 0) {
					n->lo = n->hi = p+1;
					break;
				} else if (algo <= LEAD_TRAVERSE_SEQ) {
					n->lo = p;
					n->hi = p+1;
					continue;
				}
			}
			/* Synced lookup
			 * If some BAT is synced with a BAT we already
			 * handled ('parent'), we can simply copy and
			 * convert the BUNlists of the parent.
			 */
			if ((m = n->sync) != NULL) {
				if (m->hit) {
					BUN j;

					REALLOCBUNS(n, m->nhits);
					for (j = 0; j < m->nhits; j++) {
						n->hit[j] = (BUN) (n->offset + m->hit[j]);
					}
					n->nhits = m->nhits;
				} else {
					n->lo = (BUN) (n->offset + m->lo);
					n->hi = (BUN) (n->offset + m->hi);
				}
				/* Sorted lookup
				 * We perform a merge scan over the tail column.
				 */
			} else if (n->ordered) {
				BUN last = BUNlast(b);

				if (n->binsearch) {
					n->cur = SORTfndfirst(BATmirror(b), h);
					if (n->cur >= last)
						break;	/* NOT FOUND */
				} else {
					int yy = 1;

					for (; n->cur < last; n->cur++)
						if ((yy = OIDcmp(BUNhloc(bi, n->cur), h)) >= 0)
							 break;

					if (yy != 0)
						break;	/* NOT FOUND */
				}
				n->lo = n->cur;
				for (n->nhits = 1; (++n->cur) < last; n->nhits++) {
					if (OIDcmp(BUNhloc(bi, n->cur), h))
						 break;
				}
				if (n->cur >= last && (algo & LEAD_INTERRUPT_END))
					algo = 0;
				n->hi = n->cur;
				/* Single Hash lookup
				 */
			} else if (BAThkey(n->b)) {
				BUNfndOID(n->cur, bi, h);
				if (n->cur == BUN_NONE)
					break;	/* NOT FOUND */
				n->lo = n->cur;
				n->hi = n->cur+1;
				/* Multiple Hash lookup
				 */
			} else {
				BUN j;

				n->nhits = 0;
				HASHloop_oid(bi, b->H->hash, j, h) {
					REALLOCBUNS(n, n->nhits + 1);
					n->hit[n->nhits++] = j;
				}
				if (n->nhits == 0)
					break;	/* NOT FOUND */
			}
		}
		/* Recursively print the Cartesian product of all
		 * match collections of h.
		 */
		if (i >= argc) {
			t.value_fcn[0] (t.value_data[0], h);
			column_result(&t, 0);
		} else {
			MULTIJOIN_SYNCED(status) = 0;	/* a miss occurred somewhere! */
		}
	}
	if (lead_col->hi < q) {
		MULTIJOIN_SYNCED(status) = 0;	/* an interrupt occurred! */
	}


#line 3136 "gdk_relop.mx"

	} else {
		int (*cmp) (const void *, const void *) = BATatoms[lead_col->b->htype].atomCmp;

		
#line 3210 "gdk_relop.mx"
	while (algo) {
		ptr h;		/* points to current ID */

		/*
		 * find the next leader bun
		 */
		p = lead_col->hi;
		if (p >= q)
			break;
		h = BUNhead(lead_col->bi, p);

		/* FIND MATCHING COLUMN RANGES
		 * For each column, find all matches for this head value
		 */
		for (i = 0; i < argc; i++) {
			column_t *m, *n = reorder[i];	/* use BATcount() order */
			BAT *b = n->b;
			BATiter bi = n->bi;

			/* one-by-one traversal of the lead column? => no matching done.
			 */
			if (n == lead_col) {
				if (argc > 1 && ATOMcmp(b->htype, h, ATOMnilptr(b->htype)) == 0) {
					n->lo = n->hi = p+1;
					break;
				} else if (algo <= LEAD_TRAVERSE_SEQ) {
					n->lo = p;
					n->hi = p+1;
					continue;
				}
			}
			/* Synced lookup
			 * If some BAT is synced with a BAT we already
			 * handled ('parent'), we can simply copy and
			 * convert the BUNlists of the parent.
			 */
			if ((m = n->sync) != NULL) {
				if (m->hit) {
					BUN j;

					REALLOCBUNS(n, m->nhits);
					for (j = 0; j < m->nhits; j++) {
						n->hit[j] = (BUN) (n->offset + m->hit[j]);
					}
					n->nhits = m->nhits;
				} else {
					n->lo = (BUN) (n->offset + m->lo);
					n->hi = (BUN) (n->offset + m->hi);
				}
				/* Sorted lookup
				 * We perform a merge scan over the tail column.
				 */
			} else if (n->ordered) {
				BUN last = BUNlast(b);

				if (n->binsearch) {
					n->cur = SORTfndfirst(BATmirror(b), h);
					if (n->cur >= last)
						break;	/* NOT FOUND */
				} else {
					int yy = 1;

					for (; n->cur < last; n->cur++)
						if ((yy = STDcmp(BUNhead(bi, n->cur), h)) >= 0)
							 break;

					if (yy != 0)
						break;	/* NOT FOUND */
				}
				n->lo = n->cur;
				for (n->nhits = 1; (++n->cur) < last; n->nhits++) {
					if (STDcmp(BUNhead(bi, n->cur), h))
						 break;
				}
				if (n->cur >= last && (algo & LEAD_INTERRUPT_END))
					algo = 0;
				n->hi = n->cur;
				/* Single Hash lookup
				 */
			} else if (BAThkey(n->b)) {
				BUNfndSTD(n->cur, bi, h);
				if (n->cur == BUN_NONE)
					break;	/* NOT FOUND */
				n->lo = n->cur;
				n->hi = n->cur+1;
				/* Multiple Hash lookup
				 */
			} else {
				BUN j;

				n->nhits = 0;
				HASHloop_any(bi, b->H->hash, j, h) {
					REALLOCBUNS(n, n->nhits + 1);
					n->hit[n->nhits++] = j;
				}
				if (n->nhits == 0)
					break;	/* NOT FOUND */
			}
		}
		/* Recursively print the Cartesian product of all
		 * match collections of h.
		 */
		if (i >= argc) {
			t.value_fcn[0] (t.value_data[0], h);
			column_result(&t, 0);
		} else {
			MULTIJOIN_SYNCED(status) = 0;	/* a miss occurred somewhere! */
		}
	}
	if (lead_col->hi < q) {
		MULTIJOIN_SYNCED(status) = 0;	/* an interrupt occurred! */
	}


#line 3140 "gdk_relop.mx"

	}
	/*
	 * Cleanup & exit.
	 */
	for (i = 0; i < argc; i++) {
		if (c[i].hitsize)
			GDKfree(c[i].hit);
	}
	GDKfree(c);
	GDKfree(reorder);
	return status;
}

/*
 * @+ The Matching Algorithm
 * In multi-column join, all MonetDB accelerators are put to use when
 * equi-lookup is done on a number of head columns.  In order of
 * preference, it:
 *
 * @itemize
 * @item
 *     does positional lookup on @emph{ virtual oid} columns (void).
 * @item
 *     reuses lookup info on @emph{ synced columns}.
 * @item
 *     uses merge scan on @emph{ ordered columns}.
 * @item
 *     uses binary tree leaf scan on @emph{ indexed columns}.
 * @item
 *     uses hash lookup in other cases. If a hash-table does not
 * exist; it is created on the fly.
 * @end itemize
 *
 * The algorithm goes one by one, for unique head values in the
 * smallest-sized BAT. The strategy is for each column to find a range
 * of BUNs that match it.
 *
 * The algorithm is intelligent in that it processes the columns in
 * order of cardinality. If a column has no matches, you can cut off
 * the matching process for the current ID (head value) and go to the
 * next. Smallest BATs first means highest miss probability first.
 *
 * Another optimization is that when a column has a cardinality much
 * larger than the smallest column, you can expect sparse matching
 * (e.g. you selected 1% tuples out of a 1M tuple BAT, and re-joins
 * both with this routine). In those cases the merge algorithms use
 * binary search instead of mergescan.
 *
 * In non-empty matching ranges are found in all head columns, a
 * recursive routine is used to go over all combinations of matching
 * BUNs. This recursive routine calls for every match (the Cartesian
 * product) a special-purpose routine that is passed all matching BUN
 * pointers. This sequence of calls represents the result of the
 * multijoin.
 *
 * Normally you want to perform an action on each value (like
 * formatting or copying), but many values reoccur in the same place
 * when the Cartesian product over all columns is formed.  For
 * instance, when we have 5 attributes in which each has 2 matches on
 * the current id, we have 2*2*2*2*2=32 result tuples for this one
 * id. A simple-minded strategy would then do 32*5 value actions, when
 * processing these result tuples. This multijoin reduces that to just
 * 32, by calling whenever a value is 'changed' in the
 * result-tuple-under-construction, a value specific function,
 * provided by the user. Since each column can have a different value
 * function, this also allows for factoring out type-checking
 * overhead.
 */

