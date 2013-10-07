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

#line 23 "gdk_rangejoin.mx"
/*
 * @a N. J. Nes
 *
 * @* Range Join Operators
 * The sql statement b.x <= a.z <= b.y, could be implemented using too thetajoins.
 * But that results in very large intermediates.
 */


#line 50 "gdk_rangejoin.mx"

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_rangejoin.h"
#include <math.h>

BAT *BATrangejoin(BAT *l, BAT *rl, BAT *rh, bit li, bit hi)
{
	BAT *bn;

	ERRORcheck(l == NULL, "BATrangejoin: invalid left operand");
	ERRORcheck(rl == NULL, "BATrangejoin: invalid right low operand");
	ERRORcheck(rh == NULL, "BATrangejoin: invalid right high operand");
	ERRORcheck(TYPEerror(l->ttype, rl->ttype), "BATrangejoin: type conflict\n");
	ERRORcheck(TYPEerror(l->ttype, rh->ttype), "BATrangejoin: type conflict\n");
	/* TODO check that rl and rh are aligned */

	bn = BATnew(BAThtype(l), BAThtype(rl), MIN(BATcount(l), BATcount(rl)));
	if (bn == NULL) 
		return bn;
	switch (ATOMstorage(rl->ttype)) {
	case TYPE_bte:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		bte x1 = *(bte *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(bte *) BUNtloc(rli, v))  &&
			    (x1 <= *(bte *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		bte x1 = *(bte *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(bte *) BUNtloc(rli, v))  &&
			    (x1 < *(bte *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		bte x1 = *(bte *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(bte *) BUNtloc(rli, v))  &&
			    (x1 <= *(bte *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		bte x1 = *(bte *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(bte *) BUNtloc(rli, v))  &&
			    (x1 < *(bte *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 73 "gdk_rangejoin.mx"

	case TYPE_sht:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		sht x1 = *(sht *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(sht *) BUNtloc(rli, v))  &&
			    (x1 <= *(sht *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		sht x1 = *(sht *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(sht *) BUNtloc(rli, v))  &&
			    (x1 < *(sht *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		sht x1 = *(sht *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(sht *) BUNtloc(rli, v))  &&
			    (x1 <= *(sht *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		sht x1 = *(sht *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(sht *) BUNtloc(rli, v))  &&
			    (x1 < *(sht *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 75 "gdk_rangejoin.mx"

	case TYPE_int:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		int x1 = *(int *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(int *) BUNtloc(rli, v))  &&
			    (x1 <= *(int *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		int x1 = *(int *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(int *) BUNtloc(rli, v))  &&
			    (x1 < *(int *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		int x1 = *(int *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(int *) BUNtloc(rli, v))  &&
			    (x1 <= *(int *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		int x1 = *(int *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(int *) BUNtloc(rli, v))  &&
			    (x1 < *(int *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 77 "gdk_rangejoin.mx"

	case TYPE_wrd:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		wrd x1 = *(wrd *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(wrd *) BUNtloc(rli, v))  &&
			    (x1 <= *(wrd *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		wrd x1 = *(wrd *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(wrd *) BUNtloc(rli, v))  &&
			    (x1 < *(wrd *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		wrd x1 = *(wrd *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(wrd *) BUNtloc(rli, v))  &&
			    (x1 <= *(wrd *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		wrd x1 = *(wrd *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(wrd *) BUNtloc(rli, v))  &&
			    (x1 < *(wrd *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 79 "gdk_rangejoin.mx"

	case TYPE_flt:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		flt x1 = *(flt *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(flt *) BUNtloc(rli, v))  &&
			    (x1 <= *(flt *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		flt x1 = *(flt *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(flt *) BUNtloc(rli, v))  &&
			    (x1 < *(flt *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		flt x1 = *(flt *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(flt *) BUNtloc(rli, v))  &&
			    (x1 <= *(flt *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		flt x1 = *(flt *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(flt *) BUNtloc(rli, v))  &&
			    (x1 < *(flt *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 81 "gdk_rangejoin.mx"

	case TYPE_dbl:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		dbl x1 = *(dbl *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(dbl *) BUNtloc(rli, v))  &&
			    (x1 <= *(dbl *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		dbl x1 = *(dbl *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(dbl *) BUNtloc(rli, v))  &&
			    (x1 < *(dbl *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		dbl x1 = *(dbl *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(dbl *) BUNtloc(rli, v))  &&
			    (x1 <= *(dbl *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		dbl x1 = *(dbl *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(dbl *) BUNtloc(rli, v))  &&
			    (x1 < *(dbl *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 83 "gdk_rangejoin.mx"

	case TYPE_lng:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		lng x1 = *(lng *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(lng *) BUNtloc(rli, v))  &&
			    (x1 <= *(lng *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		lng x1 = *(lng *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 >= *(lng *) BUNtloc(rli, v))  &&
			    (x1 < *(lng *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		lng x1 = *(lng *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(lng *) BUNtloc(rli, v))  &&
			    (x1 <= *(lng *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 112 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;

	BATloop(l, p, q) {
		lng x1 = *(lng *) BUNtloc(li, p);
		BATloop(rl, v, w) {
			if ((x1 > *(lng *) BUNtloc(rli, v))  &&
			    (x1 < *(lng *) BUNtloc(rhi, v))) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 85 "gdk_rangejoin.mx"

	default:
		
#line 101 "gdk_rangejoin.mx"
if (li && hi)
	
#line 144 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;

	if (!ATOMlinear(l->ttype)) {
		GDKerror("range join not possible on non-linear types\n");
		return NULL;
	}
	BATloop(l, p, q) {
		ptr x1 = (ptr)BUNtail(li, p);
		BATloop(rl, v, w) {
			if (cmp(x1, (ptr)BUNtail(rli, v)) >= 0  &&
			    cmp(x1, (ptr)BUNtail(rhi, v)) <= 0) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 102 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 144 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;

	if (!ATOMlinear(l->ttype)) {
		GDKerror("range join not possible on non-linear types\n");
		return NULL;
	}
	BATloop(l, p, q) {
		ptr x1 = (ptr)BUNtail(li, p);
		BATloop(rl, v, w) {
			if (cmp(x1, (ptr)BUNtail(rli, v)) >= 0  &&
			    cmp(x1, (ptr)BUNtail(rhi, v)) < 0) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 103 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 144 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;

	if (!ATOMlinear(l->ttype)) {
		GDKerror("range join not possible on non-linear types\n");
		return NULL;
	}
	BATloop(l, p, q) {
		ptr x1 = (ptr)BUNtail(li, p);
		BATloop(rl, v, w) {
			if (cmp(x1, (ptr)BUNtail(rli, v)) > 0  &&
			    cmp(x1, (ptr)BUNtail(rhi, v)) <= 0) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 104 "gdk_rangejoin.mx"
else
	
#line 144 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	BUN p, q;
	BUN v, w;
	BUN cap = bn->batCapacity, cnt = 0;
	int (*cmp)(const void *, const void *) = BATatoms[l->ttype].atomCmp;

	if (!ATOMlinear(l->ttype)) {
		GDKerror("range join not possible on non-linear types\n");
		return NULL;
	}
	BATloop(l, p, q) {
		ptr x1 = (ptr)BUNtail(li, p);
		BATloop(rl, v, w) {
			if (cmp(x1, (ptr)BUNtail(rli, v)) > 0  &&
			    cmp(x1, (ptr)BUNtail(rhi, v)) < 0) { 
				if (BUNfastins(bn, BUNhead(li, p), BUNhead(rli, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		/* re-adjust the capacity if needed */
		cnt++;
		if ( BATcount(bn) > cap ){
			BUN sze = (BUN)ceil((double) BATcount(bn) /cnt);
			if ( BATextend(bn, BATcount(l) * sze ) == NULL ){
				BBPreclaim(bn);
				return NULL;
			}
			cap = bn->batCapacity - sze -1; /* trigger before BATextend in mainloop */
		}
	}
}


#line 105 "gdk_rangejoin.mx"
break;


#line 87 "gdk_rangejoin.mx"

	}
	/* set sorted flags by hand, because we used BUNfastins() */
	bn->hsorted = BAThordered(l);
	bn->hrevsorted = BAThrevordered(l);
	bn->tsorted = FALSE;
	bn->trevsorted = FALSE;

	ESTIDEBUG THRprintf(GDKout, "#BATrangejoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

	return bn;
}



#line 182 "gdk_rangejoin.mx"
/*
 * @-
 * @+ Bandjoin
 * A non-equi join of two relations R and S is called a Band-join if
 * the join predicate requires the values of R to fall within a given range.
 * This kind of joins is encountered in real world domains, such as those
 * involved with time and distance.
 *
 * The boundary conditions for the bandjoin are constants or a NULL value.
 * The latter enables encoding of arbitrary theta joins using the more
 * general bandjoin.
 * Incidentally note that c1 = c2 = 0 leads to an equi-join.
 *
 * The straight forward implementation uses a nested loop.
 * The current implementation does not optimize processing, because
 * the impact of the choices is not yet clear.
 *
 * The hash indexing routines have been extended with a Band argument.
 */
static BAT *
BATbandjoin_nl(BAT *l, BAT *r, ptr c1, ptr c2, bit li, bit hi)
{
	BAT *bn;

	ERRORcheck(l == NULL, "BATbandjoin: invalid left operand");
	ERRORcheck(r == NULL, "BATbandjoin: invalid right operand");
	ERRORcheck(TYPEerror(l->ttype, r->htype), "BATbandjoin: type conflict\n");
	ALGODEBUG fprintf(stderr, "#BATbandjoin: nestedloop;\n");
	bn = BATnew(BAThtype(l), BATttype(r), MIN(BATcount(l), BATcount(r)));
	if (bn == NULL)
		return bn;
	switch (ATOMstorage(r->htype)) {
	case TYPE_bte:
		
#line 277 "gdk_rangejoin.mx"
if (li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	bte *x1, *x2, cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (bte *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (bte *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 278 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	bte *x1, *x2, cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (bte *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (bte *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 279 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	bte *x1, *x2, cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (bte *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (bte *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 280 "gdk_rangejoin.mx"
else
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	bte *x1, *x2, cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (bte *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (bte *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 281 "gdk_rangejoin.mx"
break;


#line 215 "gdk_rangejoin.mx"

	case TYPE_sht:
		
#line 277 "gdk_rangejoin.mx"
if (li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	sht *x1, *x2, cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (sht *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (sht *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 278 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	sht *x1, *x2, cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (sht *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (sht *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 279 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	sht *x1, *x2, cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (sht *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (sht *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 280 "gdk_rangejoin.mx"
else
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	sht *x1, *x2, cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (sht *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (sht *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 281 "gdk_rangejoin.mx"
break;


#line 217 "gdk_rangejoin.mx"

	case TYPE_int:
		
#line 277 "gdk_rangejoin.mx"
if (li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	int *x1, *x2, cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (int *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (int *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 278 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	int *x1, *x2, cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (int *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (int *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 279 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	int *x1, *x2, cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (int *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (int *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 280 "gdk_rangejoin.mx"
else
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	int *x1, *x2, cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (int *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (int *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 281 "gdk_rangejoin.mx"
break;


#line 219 "gdk_rangejoin.mx"

	case TYPE_wrd:
		
#line 277 "gdk_rangejoin.mx"
if (li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	wrd *x1, *x2, cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (wrd *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (wrd *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 278 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	wrd *x1, *x2, cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (wrd *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (wrd *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 279 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	wrd *x1, *x2, cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (wrd *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (wrd *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 280 "gdk_rangejoin.mx"
else
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	wrd *x1, *x2, cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (wrd *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (wrd *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 281 "gdk_rangejoin.mx"
break;


#line 221 "gdk_rangejoin.mx"

	case TYPE_flt:
		
#line 277 "gdk_rangejoin.mx"
if (li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	flt *x1, *x2, cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (flt *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (flt *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 278 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	flt *x1, *x2, cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (flt *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (flt *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 279 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	flt *x1, *x2, cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (flt *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (flt *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 280 "gdk_rangejoin.mx"
else
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	flt *x1, *x2, cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (flt *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (flt *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 281 "gdk_rangejoin.mx"
break;


#line 223 "gdk_rangejoin.mx"

	case TYPE_dbl:
		
#line 277 "gdk_rangejoin.mx"
if (li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	dbl *x1, *x2, cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (dbl *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (dbl *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 278 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	dbl *x1, *x2, cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (dbl *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (dbl *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 279 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	dbl *x1, *x2, cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (dbl *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (dbl *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 280 "gdk_rangejoin.mx"
else
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	dbl *x1, *x2, cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (dbl *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (dbl *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 281 "gdk_rangejoin.mx"
break;


#line 225 "gdk_rangejoin.mx"

	case TYPE_lng:
		
#line 277 "gdk_rangejoin.mx"
if (li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	lng *x1, *x2, cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (lng *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (lng *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 278 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	lng *x1, *x2, cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (lng *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (lng *) BUNhloc(ri, v);
			if ((*x1 >= *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 279 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	lng *x1, *x2, cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (lng *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (lng *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 <= *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 280 "gdk_rangejoin.mx"
else
	
#line 256 "gdk_rangejoin.mx"
{
	BATiter li = bat_iterator(l), ri = bat_iterator(r);
	lng *x1, *x2, cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN p, q, v, w;

	BATloop(l, p, q) {
		x1 = (lng *) BUNtloc(li, p);
		BATloop(r, v, w) {
			x2 = (lng *) BUNhloc(ri, v);
			if ((*x1 > *x2 - cc1) &&
			    (*x1 < *x2 + cc2)) {
				if (BUNfastins(bn, BUNhead(li, p), BUNtail(ri, v)) == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
	}
}



#line 281 "gdk_rangejoin.mx"
break;


#line 227 "gdk_rangejoin.mx"

	default:
		(void) c1;
		(void) c2;
		(void) li;
		(void) hi;
		GDKerror("BATbandjoin: type not implemented\n");
		return NULL;
	}
	/* set sorted flags by hand, because we used BUNfastins() */
	bn->hsorted = BAThordered(l);
	bn->hrevsorted = BAThrevordered(l);
	bn->tsorted = FALSE;
	bn->trevsorted = FALSE;
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = r->T->nonil;

	ESTIDEBUG THRprintf(GDKout, "#BATbandjoin: actual resultsize: " BUNFMT "\n", BATcount(bn));

	return bn;
}

/*
 * @-
 * The easiest case is to implement a nested loop for band operations.
 * Choice point is to determine the status of the NULL values in the final
 * result.
 */


#line 360 "gdk_rangejoin.mx"

static BAT *
BATbandjoin_mj(BAT *l, BAT *r, ptr c1, ptr c2, bit li, bit hi, BUN *limit)
{
	BAT *bn = NULL;
	BUN lc = BATcount(l), rc = BATcount(r);

	ALGODEBUG fprintf(stderr, "#BATbandjoin: mergejoin;\n");
	bn = BATnew(ATOMtype(l->htype), ATOMtype(r->ttype), limit ? *limit : MIN(lc, rc));
	switch (l->ttype) {
	case TYPE_bte:
		
#line 349 "gdk_rangejoin.mx"
if (li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	bte *lt = (bte*)Tloc(l, BUNfirst(l)), *rh = (bte*)Hloc(r, BUNfirst(r)); 
	bte cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 350 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	bte *lt = (bte*)Tloc(l, BUNfirst(l)), *rh = (bte*)Hloc(r, BUNfirst(r)); 
	bte cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 351 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	bte *lt = (bte*)Tloc(l, BUNfirst(l)), *rh = (bte*)Hloc(r, BUNfirst(r)); 
	bte cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 352 "gdk_rangejoin.mx"
else
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	bte *lt = (bte*)Tloc(l, BUNfirst(l)), *rh = (bte*)Hloc(r, BUNfirst(r)); 
	bte cc1 = *(bte*)c1, cc2 = *(bte*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 353 "gdk_rangejoin.mx"
break;


#line 371 "gdk_rangejoin.mx"

	case TYPE_sht:
		
#line 349 "gdk_rangejoin.mx"
if (li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	sht *lt = (sht*)Tloc(l, BUNfirst(l)), *rh = (sht*)Hloc(r, BUNfirst(r)); 
	sht cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 350 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	sht *lt = (sht*)Tloc(l, BUNfirst(l)), *rh = (sht*)Hloc(r, BUNfirst(r)); 
	sht cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 351 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	sht *lt = (sht*)Tloc(l, BUNfirst(l)), *rh = (sht*)Hloc(r, BUNfirst(r)); 
	sht cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 352 "gdk_rangejoin.mx"
else
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	sht *lt = (sht*)Tloc(l, BUNfirst(l)), *rh = (sht*)Hloc(r, BUNfirst(r)); 
	sht cc1 = *(sht*)c1, cc2 = *(sht*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 353 "gdk_rangejoin.mx"
break;


#line 373 "gdk_rangejoin.mx"

	case TYPE_int:
		
#line 349 "gdk_rangejoin.mx"
if (li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	int *lt = (int*)Tloc(l, BUNfirst(l)), *rh = (int*)Hloc(r, BUNfirst(r)); 
	int cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 350 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	int *lt = (int*)Tloc(l, BUNfirst(l)), *rh = (int*)Hloc(r, BUNfirst(r)); 
	int cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 351 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	int *lt = (int*)Tloc(l, BUNfirst(l)), *rh = (int*)Hloc(r, BUNfirst(r)); 
	int cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 352 "gdk_rangejoin.mx"
else
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	int *lt = (int*)Tloc(l, BUNfirst(l)), *rh = (int*)Hloc(r, BUNfirst(r)); 
	int cc1 = *(int*)c1, cc2 = *(int*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 353 "gdk_rangejoin.mx"
break;


#line 375 "gdk_rangejoin.mx"

	case TYPE_wrd:
		
#line 349 "gdk_rangejoin.mx"
if (li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	wrd *lt = (wrd*)Tloc(l, BUNfirst(l)), *rh = (wrd*)Hloc(r, BUNfirst(r)); 
	wrd cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 350 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	wrd *lt = (wrd*)Tloc(l, BUNfirst(l)), *rh = (wrd*)Hloc(r, BUNfirst(r)); 
	wrd cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 351 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	wrd *lt = (wrd*)Tloc(l, BUNfirst(l)), *rh = (wrd*)Hloc(r, BUNfirst(r)); 
	wrd cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 352 "gdk_rangejoin.mx"
else
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	wrd *lt = (wrd*)Tloc(l, BUNfirst(l)), *rh = (wrd*)Hloc(r, BUNfirst(r)); 
	wrd cc1 = *(wrd*)c1, cc2 = *(wrd*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 353 "gdk_rangejoin.mx"
break;


#line 377 "gdk_rangejoin.mx"

	case TYPE_flt:
		
#line 349 "gdk_rangejoin.mx"
if (li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	flt *lt = (flt*)Tloc(l, BUNfirst(l)), *rh = (flt*)Hloc(r, BUNfirst(r)); 
	flt cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 350 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	flt *lt = (flt*)Tloc(l, BUNfirst(l)), *rh = (flt*)Hloc(r, BUNfirst(r)); 
	flt cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 351 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	flt *lt = (flt*)Tloc(l, BUNfirst(l)), *rh = (flt*)Hloc(r, BUNfirst(r)); 
	flt cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 352 "gdk_rangejoin.mx"
else
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	flt *lt = (flt*)Tloc(l, BUNfirst(l)), *rh = (flt*)Hloc(r, BUNfirst(r)); 
	flt cc1 = *(flt*)c1, cc2 = *(flt*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 353 "gdk_rangejoin.mx"
break;


#line 379 "gdk_rangejoin.mx"

	case TYPE_dbl:
		
#line 349 "gdk_rangejoin.mx"
if (li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	dbl *lt = (dbl*)Tloc(l, BUNfirst(l)), *rh = (dbl*)Hloc(r, BUNfirst(r)); 
	dbl cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 350 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	dbl *lt = (dbl*)Tloc(l, BUNfirst(l)), *rh = (dbl*)Hloc(r, BUNfirst(r)); 
	dbl cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 351 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	dbl *lt = (dbl*)Tloc(l, BUNfirst(l)), *rh = (dbl*)Hloc(r, BUNfirst(r)); 
	dbl cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 352 "gdk_rangejoin.mx"
else
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	dbl *lt = (dbl*)Tloc(l, BUNfirst(l)), *rh = (dbl*)Hloc(r, BUNfirst(r)); 
	dbl cc1 = *(dbl*)c1, cc2 = *(dbl*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 353 "gdk_rangejoin.mx"
break;


#line 381 "gdk_rangejoin.mx"

	case TYPE_lng:
		
#line 349 "gdk_rangejoin.mx"
if (li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	lng *lt = (lng*)Tloc(l, BUNfirst(l)), *rh = (lng*)Hloc(r, BUNfirst(r)); 
	lng cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 350 "gdk_rangejoin.mx"
else if (li && !hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	lng *lt = (lng*)Tloc(l, BUNfirst(l)), *rh = (lng*)Hloc(r, BUNfirst(r)); 
	lng cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] >= (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 351 "gdk_rangejoin.mx"
else if (!li && hi)
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	lng *lt = (lng*)Tloc(l, BUNfirst(l)), *rh = (lng*)Hloc(r, BUNfirst(r)); 
	lng cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] <= (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] <= (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 352 "gdk_rangejoin.mx"
else
	
#line 324 "gdk_rangejoin.mx"
{
	BUN lp = 0, rp = 0, bnf = BUNfirst(bn);
	oid *bnh = NULL, *bnt = NULL;
	bit lo = l->htype, ro = r->ttype;
	oid *lh = (oid*)Hloc(l, BUNfirst(l)), *rt = (oid*)Tloc(r, BUNfirst(r));
	lng *lt = (lng*)Tloc(l, BUNfirst(l)), *rh = (lng*)Hloc(r, BUNfirst(r)); 
	lng cc1 = *(lng*)c1, cc2 = *(lng*)c2;
	BUN ll = lp, o = 0;
	oid lhb = l->hseqbase, rtb = r->tseqbase;
	

	/* handle oid and void inputs */
	if (lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 337 "gdk_rangejoin.mx"
	} else if (!lo && ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rt[rp];
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 338 "gdk_rangejoin.mx"
	} else if (lo && !ro) {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lh[lp+i];
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 339 "gdk_rangejoin.mx"
	} else {
		
#line 288 "gdk_rangejoin.mx"
	/* sliding window like solution */
	while(lp < lc && rp < rc) {
		BUN i;

		/* slide r */
		while(rp<rc && !(lt[lp] < (rh[rp] + cc2)))
			rp++;
		if (rp>=rc)
			break;

		/* slide window */
		while(lp<lc && !(lt[lp] > (rh[rp] - cc1)))
			lp++;
		while(ll<lc && (lt[ll] < (rh[rp] + cc2)))
			ll++;
		if (o + ll-lp > BATcapacity(bn)) {
			BUN g = MAX(ll-lp, 1024*1024);
			BATsetcount(bn, o);
			if (limit) {
                              *limit = o && lc < rc ? (rp - rc) / MAX(1, rc) : (lp - lc) / MAX(1, lc);
			      break;
                        }
			if (BATextend(bn, BATcapacity(bn)+g) == NULL)
			      goto failed;
		}
		bnh = (oid*)Hloc(bn, bnf);
		bnt = (oid*)Tloc(bn, bnf);
		for (i=0; i<(ll-lp); i++, o++) {
			bnh[o] = lhb+lp+i;
			bnt[o] = rtb+rp;
		}
		rp++;
	}

/* lets assume result is oid,oid */


#line 340 "gdk_rangejoin.mx"
	}
	BATsetcount(bn, o);
}



#line 353 "gdk_rangejoin.mx"
break;


#line 383 "gdk_rangejoin.mx"

	default:
		(void) c1;
		(void) c2;
		(void) li;
		(void) hi;
		GDKerror("BATbandjoin: type not implemented\n");
		goto failed;
	}
	BATkey(bn, FALSE);
	BATkey(BATmirror(bn), FALSE);
	bn->hsorted = FALSE;
	bn->hrevsorted = FALSE;
	bn->tsorted = BATtordered(r);
	bn->trevsorted = BATtrevordered(r);
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = r->T->nonil;
	return bn;
      failed:
	BBPreclaim(bn);
	return NULL;
}

static BAT *
BATbandmergejoin_limit(BAT *l, BAT *r, ptr c1, ptr c2, bit li, bit hi, BUN *limit)
{
	BAT *ol = l, *or = r, *bn = NULL;

	/* large cases use a merge join like band, therefore we first sort */
	if (!BATtordered(l))
		l = BATmirror(BATsort(BATmirror(l)));
	if (!BAThordered(r))
		r = BATsort(r);

	bn = BATbandjoin_mj(l, r, c1, c2, li, hi, limit);

	if (l != ol)
		BBPreclaim(l);
	if (r != or)
		BBPreclaim(r);
	return bn;
}

static BAT *
BATbandmergejoin(BAT *l, BAT *r, ptr c1, ptr c2, bit li, bit hi)
{
	return BATbandmergejoin_limit(l, r, c1, c2, li, hi, NULL);
}

BAT *
BATbandjoin(BAT *l, BAT *r, ptr c1, ptr c2, bit li, bit hi)
{
	if ((!BATtordered(l) && BATcount(l) < BATTINY) ||
	    (!BAThordered(r) && BATcount(r) < BATTINY) ||
	    ATOMtype(l->htype) != TYPE_oid ||
	    ATOMtype(r->ttype) != TYPE_oid)
		return BATbandjoin_nl(l, r, c1, c2, li, hi);

	return BATbandmergejoin(l, r, c1, c2, li, hi);
}


