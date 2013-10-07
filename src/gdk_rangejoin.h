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

#ifndef GDK_RANGEJOIN_H
#define GDK_RANGEJOIN_H

/*
 * @- BAT range and band join operators
 * The BATbandjoin produces the associations [A, D] such that S.C-c1 <=
 * R.b <= S.C + c2.  The special case c1 = 0 and c2 = infinite leads to a
 * thetajoin.
 */
gdk_export BAT *BATrangejoin(BAT *l, BAT *rl, BAT *rh, bit li, bit hi);
/*
 * Join all BUNs of the BATs that have tail values: {rl <= l <= rh}.
 */
gdk_export BAT *BATbandjoin(BAT *l, BAT *r, ptr mnus, ptr plus, bit li, bit hi);
/*
 * Versions of bandjoin fixed on the merge implementation (possibly with a cut-off limit);
 */
#endif /* GDK_RANGEJOIN_H */

