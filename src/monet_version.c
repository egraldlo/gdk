/* -*-C-*- */
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

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_utils.h"
#include <stdio.h>
#include "monet_version.h"
#include <pcre.h>
#include <openssl/opensslv.h>
//#include <libxml/xmlversion.h>

void
monet_version(void)
{
	dbl sz_mem_gb;
	int cores;

	MT_init();  /* for MT_pagesize */
	sz_mem_gb = (dbl)(MT_npages() * MT_pagesize()) / (1024.0 * 1024.0 * 1024.0);
	cores = MT_check_nr_cores();

	printf("MonetDB 5 server v" VERSION " ");
	if (strcmp(MONETDB_RELEASE, "unreleased") != 0)
		printf("\"%s\" ", MONETDB_RELEASE);
	printf("(" SZFMT "-bit, " SZFMT "-bit oids)\n",
			(size_t) (sizeof(ptr) * 8),
			(size_t) (sizeof(oid) * 8));
	if (strcmp(MONETDB_RELEASE, "unreleased") == 0)
		printf("This is an unreleased version\n");
	printf("Copyright (c) 1993-July 2008 CWI\n"
			"Copyright (c) August 2008-2013 MonetDB B.V., all rights reserved\n");
	printf("Visit http://www.monetdb.org/ for further information\n");
	printf("Found %.1fGiB available memory, %d available cpu core%s\n",
			sz_mem_gb, cores, cores != 1 ? "s" : "");
	/* don't want to GDKinit just for this
			"using %d thread%s\n",
			GDKnr_threads, GDKnr_threads != 1 ? "s" : ""); */
	printf("Libraries:\n");
	{
		char pcreversion[] = "compiled with 7.8";
		printf("  libpcre: %s", pcre_version());
		if (*pcreversion)
			printf(" (%s)", pcreversion);
		printf("\n");
	}
	{
		char opensslversion[] = "compiled with OpenSSL 1.0.0-fips 29 Mar 2010";
		printf("  openssl: %s", OPENSSL_VERSION_TEXT);
		if (*opensslversion)
			printf(" (%s)", opensslversion);
		printf("\n");
	}
	{
		char libxml2version[] = "compiled with 2.7.6";
//		printf("  libxml2: %s", LIBXML_DOTTED_VERSION);
		if (*libxml2version)
			printf(" (%s)", libxml2version);
		printf("\n");
	}
	printf("Compiled by: %s (" HOST ")\n", "casa@zhanglei");
	printf("Compilation: %s\n", "gcc -g -O2 ");
#ifdef MONETDB_STATIC
	printf("Linking    : %s (static)\n", "/usr/bin/ld -m elf_x86_64 ");
#else
	printf("Linking    : %s\n", "/usr/bin/ld -m elf_x86_64 ");
#endif
}
