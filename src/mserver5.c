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
#include <stdio.h>
#include <errno.h>
#include <string.h> /* strerror */
#include <locale.h>
#include "gdk.h"
#include "rdtsc.h"
#include "monet_options.h"
#include "monet_version.h"
#include "gdk_utils.h"
#include "mutils.h"
#include "monet_debug.h"

#ifdef HAVE_LIBGEN_H
#include <libgen.h>
#endif

#ifndef HAVE_GETOPT_LONG
#  include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif



#define GRPthreads (THRDMASK | PARMASK)
#define GRPmemory (MEMMASK | ALLOCMASK )
#define GRPproperties (CHECKMASK | PROPMASK | BATMASK )
#define GRPio (IOMASK | PERFMASK )
#define GRPheaps (HEAPMASK)
#define GRPtransactions (TMMASK | DELTAMASK | TEMMASK)
#define GRPmodules (LOADMASK)
#define GRPalgorithms (ALGOMASK | ESTIMASK)
#define GRPxproperties 0 /* (XPROPMASK) */
#define GRPperformance (JOINPROPMASK | DEADBEEFMASK)
#define GRPoptimizers  (OPTMASK)
#define GRPforcemito (FORCEMITOMASK)

static int malloc_init = 1;
char monet_cwd[4096] = { 0 };//borrowing
static int monet_daemon;

/* NEEDED? */
#if defined(_MSC_VER) && defined(__cplusplus)
#include <eh.h>
void
mserver_abort()
{
	fprintf(stderr, "\n! mserver_abort() was called by terminate(). !\n");
	fflush(stderr);
	MT_global_exit(0);
}
#endif

static void usage(char *prog, int xit)
__attribute__((__noreturn__));

// 时间测量
unsigned long long timer;
void *timer_(void *args){
	int i=0;
	while(i>=0){
		sleep(1);
		i++;
		printf("time: %d\n",i);
	}
	return (void *)(0);
}

static void
usage(char *prog, int xit)
{
	fprintf(stderr, "Usage: %s [options] [scripts]\n", prog);
	fprintf(stderr, "    --dbpath=<directory>      Specify database location\n");
	fprintf(stderr, "    --dbinit=<stmt>           Execute statement at startup\n");
	fprintf(stderr, "    --config=<config_file>    Use config_file to read options from\n");
	fprintf(stderr, "    --daemon=yes|no           Do not read commands from standard input [no]\n");
	fprintf(stderr, "    --single-user             Allow only one user at a time\n");
	fprintf(stderr, "    --readonly                Safeguard database\n");
	fprintf(stderr, "    --set <option>=<value>    Set configuration option\n");
	fprintf(stderr, "    --help                    Print this list of options\n");
	fprintf(stderr, "    --version                 Print version and compile time info\n");

	fprintf(stderr, "The debug, testing & trace options:\n");
	fprintf(stderr, "     --threads\n");
	fprintf(stderr, "     --memory\n");
	fprintf(stderr, "     --io\n");
	fprintf(stderr, "     --heaps\n");
	fprintf(stderr, "     --properties\n");
	fprintf(stderr, "     --transactions\n");
	fprintf(stderr, "     --modules\n");
	fprintf(stderr, "     --algorithms\n");
#if 0
	fprintf(stderr, "     --xproperties\n");
#endif
	fprintf(stderr, "     --performance\n");
	fprintf(stderr, "     --optimizers\n");
	fprintf(stderr, "     --forcemito\n");
	fprintf(stderr, "     --debug=<bitmask>\n");

	exit(xit);
}

static void monet_hello_(void){
	dbl sz_mem_h;
	size_t monet_memory;
	int qi = 0;
#ifdef MONETDB_STATIC
	char *linkinfo = "statically";
#else
	char *linkinfo = "dynamically";
#endif
	monet_memory = MT_npages() * MT_pagesize();
	sz_mem_h = (dbl) monet_memory;
	while (sz_mem_h >= 1000.0 && qi < 6) {
		sz_mem_h /= 1024.0;
		qi++;
	}
	char  *qc = " kMGTPE";
	printf("\n**********welcome to gdk!**********");
	printf("\n# Serving database '%s', using %d thread%s\n",GDKgetenv("gdk_dbname")
			,GDKnr_threads, (GDKnr_threads != 1) ? "s" : "");
	printf("# Compiled for %s/" SZFMT "bit \n",
			HOST, sizeof(ptr) * 8);
	printf("# "SZFMT "bit OIDs %s linked\n", sizeof(oid) * 8, linkinfo);
	printf("# Found %.3f %ciB available main-memory.\n",sz_mem_h, qc[qi]);
}

static void
monet_hello(void)
{
	dbl sz_mem_h;
	size_t monet_memory;//borrow
#ifdef MONETDB_STATIC
	char *linkinfo = "statically";
#else
	char *linkinfo = "dynamically";
#endif

	int qi = 0;

	monet_memory = MT_npages() * MT_pagesize();
	sz_mem_h = (dbl) monet_memory;
	while (sz_mem_h >= 1000.0 && qi < 6) {
		sz_mem_h /= 1024.0;
		qi++;
	}
	char  *qc = " kMGTPE";
	printf("# MonetDB 5 server v" VERSION);
	printf("\n# Serving database '%s', using %d thread%s\n",
			GDKgetenv("gdk_dbname"),
			GDKnr_threads, (GDKnr_threads != 1) ? "s" : "");
#ifdef MONET_GLOBAL_DEBUG
	printf("# Database path:%s\n", GDKgetenv("gdk_dbpath"));
	printf("# Module path:%s\n", GDKgetenv("monet_mod_path"));
#endif
	printf("# Compiled for %s/" SZFMT "bit with " SZFMT "bit OIDs %s linked\n",
			HOST, sizeof(ptr) * 8, sizeof(oid) * 8, linkinfo);
	printf("# Copyright (c) 1993-July 2008 CWI.\n");
	printf("# Copyright (c) August 2008-2013 MonetDB B.V., all rights reserved\n");
	printf("# Visit http://www.monetdb.org/ for further information\n");
	printf("# Found %.3f %ciB available main-memory.\n",sz_mem_h, qc[qi]);
}

static str
absolute_path(str s)
{
	if (!MT_path_absolute(s)) {
		str ret = (str) GDKmalloc(strlen(s) + strlen(monet_cwd) + 2);

		if (ret)
			sprintf(ret, "%s%c%s", monet_cwd, DIR_SEP, s);
		return ret;
	}
	return GDKstrdup(s);
}

#define BSIZE 8192

static int
monet_init(opt *set, int setlen)
{
	/* determine Monet's kernel settings */
	monet_print("the init of gdk");
	if (!GDKinit(set, setlen))
		return 0;

#ifdef HAVE_CONSOLE
	monet_daemon = 0;
	if (GDKgetenv_isyes("monet_daemon")) {
		monet_daemon = 1;
#ifdef HAVE_SETSID
		setsid();
#endif
	}
#endif
	/*
	 * change the monet_hello to monet_hello_
	 * */
	monet_hello_();

	return 1;
}

//static void emergencyBreakpoint(void)
//{
//	/* just a handle to break after system initialization for GDB */
//}

//static void
//handler(int sig)
//{
//	(void) sig;
//	mal_exit();
//}

int
main(int argc, char **av)
{
	char *prog = *av;
	opt *set = NULL;
	int idx = 0, grpdebug = 0, debug = 0, setlen = 0;//, listing = 0, i = 0;
	str dbinit = NULL;
//	str err = MAL_SUCCEED;
//	char prmodpath[1024];
//	char *modpath = NULL;
	char *binpath = NULL;
	str *monet_script;

	static struct option long_options[] = {
		{ "config", 1, 0, 'c' },
		{ "dbpath", 1, 0, 0 },
		{ "dbinit", 1, 0, 0 },
		{ "daemon", 1, 0, 0 },
		{ "debug", 2, 0, 'd' },
		{ "help", 0, 0, '?' },
		{ "version", 0, 0, 0 },
		{ "readonly", 0, 0, 'r' },
		{ "single-user", 0, 0, 0 },
		{ "set", 1, 0, 's' },
		{ "threads", 0, 0, 0 },
		{ "memory", 0, 0, 0 },
		{ "properties", 0, 0, 0 },
		{ "io", 0, 0, 0 },
		{ "transactions", 0, 0, 0 },
		{ "modules", 0, 0, 0 },
		{ "algorithms", 0, 0, 0 },
		{ "optimizers", 0, 0, 0 },
		{ "performance", 0, 0, 0 },
#if 0
		{ "xproperties", 0, 0, 0 },
#endif
		{ "forcemito", 0, 0, 0 },
		{ "heaps", 0, 0, 0 },
		{ 0, 0, 0, 0 }
	};

#if defined(_MSC_VER) && defined(__cplusplus)
	set_terminate(mserver_abort);
#endif
	if (setlocale(LC_CTYPE, "") == NULL) {
		GDKfatal("cannot set locale\n");
	}

#ifdef HAVE_MALLOPT
	if (malloc_init) {
/* for (Red Hat) Linux (6.2) unused and ignored at least as of glibc-2.1.3-15 */
/* for (Red Hat) Linux (8) used at least as of glibc-2.2.93-5 */
		if (mallopt(M_MXFAST, 192)) {
			fprintf(stderr, "!monet: mallopt(M_MXFAST,192) fails.\n");
		}
#ifdef M_BLKSZ
		if (mallopt(M_BLKSZ, 8 * 1024)) {
			fprintf(stderr, "!monet: mallopt(M_BLKSZ,8*1024) fails.\n");
		}
#endif
	}
	malloc_init = 0;
#else
	(void) malloc_init; /* still unused */
#endif

	if (getcwd(monet_cwd, PATHLENGTH - 1) == NULL) {
		perror("pwd");
		GDKfatal("monet_init: could not determine current directory\n");
	}

	/* retrieve binpath early (before monet_init) because some
	 * implementations require the working directory when the binary was
	 * called */
	binpath = get_bin_path();

	if (!(setlen = mo_builtin_settings(&set)))
		usage(prog, -1);

	for (;;) {
		int option_index = 0;

		int c = getopt_long(argc, av, "c:d::rs:?",
				long_options, &option_index);

		if (c == -1)
			break;

		switch (c) {
		case 0:
			if (strcmp(long_options[option_index].name, "dbpath") == 0) {
				size_t optarglen = strlen(optarg);
				/* remove trailing directory separator */
				while (optarglen > 0 &&
				       (optarg[optarglen - 1] == '/' ||
					optarg[optarglen - 1] == '\\'))
					optarg[--optarglen] = '\0';
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", optarg);
				break;
			}
			if (strcmp(long_options[option_index].name, "dbinit") == 0) {
				if (dbinit)
					fprintf(stderr, "#warning: ignoring multiple --dbinit argument\n");
				else
					dbinit = optarg;
				break;
			}
#ifdef HAVE_CONSOLE
			if (strcmp(long_options[option_index].name, "daemon") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "monet_daemon", optarg);
				break;
			}
#endif
			if (strcmp(long_options[option_index].name, "single-user") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_single_user", "yes");
				break;
			}
			if (strcmp(long_options[option_index].name, "version") == 0) {
				monet_version();
				exit(0);
			}
			/* debugging options */
			if (strcmp(long_options[option_index].name, "properties") == 0) {
				grpdebug |= GRPproperties;
				break;
			}
			if (strcmp(long_options[option_index].name, "algorithms") == 0) {
				grpdebug |= GRPalgorithms;
				break;
			}
			if (strcmp(long_options[option_index].name, "optimizers") == 0) {
				grpdebug |= GRPoptimizers;
				break;
			}
#if 0
			if (strcmp(long_options[option_index].name, "xproperties") == 0) {
				grpdebug |= GRPxproperties;
				break;
			}
#endif
			if (strcmp(long_options[option_index].name, "forcemito") == 0) {
				grpdebug |= GRPforcemito;
				break;
			}
			if (strcmp(long_options[option_index].name, "performance") == 0) {
				grpdebug |= GRPperformance;
				break;
			}
			if (strcmp(long_options[option_index].name, "io") == 0) {
				grpdebug |= GRPio;
				break;
			}
			if (strcmp(long_options[option_index].name, "memory") == 0) {
				grpdebug |= GRPmemory;
				break;
			}
			if (strcmp(long_options[option_index].name, "modules") == 0) {
				grpdebug |= GRPmodules;
				break;
			}
			if (strcmp(long_options[option_index].name, "transactions") == 0) {
				grpdebug |= GRPtransactions;
				break;
			}
			if (strcmp(long_options[option_index].name, "threads") == 0) {
				grpdebug |= GRPthreads;
				break;
			}
			if (strcmp(long_options[option_index].name, "heaps") == 0) {
				grpdebug |= GRPheaps;
				break;
			}
			usage(prog, -1);
		/* not reached */
		case 'c':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "config", optarg);
			break;
		case 'd':
			if (optarg) {
				debug |= strtol(optarg, NULL, 10);
			} else {
				debug |= 1;
			}
			break;
		case 'r':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_readonly", "yes");
			break;
		case 's': {
			/* should add option to a list */
			char *tmp = strchr(optarg, '=');

			if (tmp) {
				*tmp = '\0';
				setlen = mo_add_option(&set, setlen, opt_cmdline, optarg, tmp + 1);
			} else {
				fprintf(stderr, "ERROR: wrong format %s\n", optarg);
			}
		}
		break;
		case '?':
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			usage(prog, strcmp(av[optind - 1], "-?") == 0 || strcmp(av[optind - 1], "--help") == 0 ? 0 : -1);
		default:
			fprintf(stderr, "ERROR: getopt returned character "
							"code '%c' 0%o\n", c, c);
			usage(prog, -1);
		}
	}

	if (!(setlen = mo_system_config(&set, setlen)))
		usage(prog, -1);


	if (debug || grpdebug) {
		long_str buf;

		if (debug)
			mo_print_options(set, setlen);
		debug |= grpdebug;  /* add the algorithm tracers */
		snprintf(buf, sizeof(long_str) - 1, "%d", debug);
		setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_debug", buf);
	}

	monet_script = (str *) malloc(sizeof(str) * (argc + 1));
	if (monet_script) {
		monet_script[idx] = NULL;
		while (optind < argc) {
			monet_script[idx] = absolute_path(av[optind]);
			monet_script[idx + 1] = NULL;
			optind++;
			idx++;
		}
	}

	monet_print("the init of monetdb");
	if (monet_init(set, setlen) == 0) {
		mo_free_options(set, setlen);
		return 0;
	}

	/*
	 * 测试区
	 * */

	printf("\n\n测试开始...\n\n");
	//1, 测试BATnew
	BAT *b_;
	if(b_=BATnew(5,5,1000)){
		monet_integer("b_->batCacheid",b_->batCacheid);
	}
	//3, 测试插入BUNinsert
	/*问题：第四个参数是干什么的？
	 *     为什么只有在插入足够多的情况下才能在磁盘上持久化呢？
	 */
	int i=320;
	while(i--){
		BUNins(b_,&i,&i,1);
	}
	//2, 测试TMcommit
	TMcommit();
	//4, 测试BATmode
	BATmode(b_,PERSISTENT);
	//5, 测试BATsave
	BATsave(b_);
	//6, 测试mergejoin
	BAT *mergejoinbat;
	BAT *join1,*join2,*join3,*join4;
	join1=BATnew(5,5,100);
	int i1=1,i2=2,i3=3,i4=4,i5=1,i6=5,i7=4,i8=2,i9=4,i10=5,i11=5,i12=3;
	if(BUNins(join1,&i1,&i2,FALSE)==0);
	if(BUNins(join1,&i3,&i4,FALSE)==0);
	if(BUNins(join1,&i9,&i10,FALSE)==0);
	printf("in join1\n");
	if(BATprint(join1)==0);
	join2=BATnew(5,5,100);
	if(BUNins(join2,&i5,&i6,FALSE)==0);
	if(BUNins(join2,&i7,&i8,FALSE)==0);
	if(BUNins(join2,&i11,&i12,FALSE)==0);
	printf("in join2\n");
	if(BATprint(join2)==0);
	printf("in mergejoin\n");
	mergejoinbat=BATmergejoin(join1,join2,100);
	if(BATprint(mergejoinbat)==0);

	int i13=2,i14=1,i15=4,i16=9;
	join3=BATnew(5,5,100);
	if(BUNins(join3,&i13,&i14,FALSE)==0);
	if(BUNins(join3,&i15,&i16,FALSE)==0);
	printf("in join3\n");
	if(BATprint(join3)==0);

	join4=BATmergejoin(mergejoinbat,join3,100);
	printf("in join4\n");
	if(BATprint(join4)==0);
	printf("let us see the memory,changed?\n");

	// 7, 测试排序
	printf("enter the number of sort:\n");
	int number;
	scanf("%d",&number);
	printf("begin to sort...\n");
//	sleep(3);
	BAT *sort=BATnew(5,5,number);
	int sort_num=0;
	int left,right;
	for(sort_num=0;sort_num<number;sort_num++){
		left=rand()%number;
		right=rand()%number;
		BUNins(sort,&left,&right,FALSE);
	}
	printf("sort result...\n");
//	sleep(3);
	/*
	 * 结果表示BATsort函数是以head来排序的
	 * */
	pthread_t pid;
	pthread_create(&pid,NULL,timer_,NULL);
	startTimer(&timer);
	BAT *finish_sort=BATsort(sort);
	stopTimer(&timer);
	printf("time consuming: %lld, %f\n",timer,timer/(double)CPU_FRE);
	pthread_cancel(pid);
//	if(BATprint(finish_sort)==0);

	// 8, 查看数据类型 gdk.h:469 line
	// #define TYPE_void	0
    // #define TYPE_bit	1

	/*
	 * 测试区
	 * */
	return 0;
}
