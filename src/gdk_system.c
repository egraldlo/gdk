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
 * @a Niels Nes, Peter Boncz
 * @+ Threads
 * This file contains a wrapper layer for threading, hence the
 * underscore convention MT_x (Multi-Threading).  As all platforms
 * that MonetDB runs on now support POSIX Threads (pthreads), this
 * wrapping layer has become rather thin.
 *
 * In the late 1990s when multi-threading support was introduced in
 * MonetDB, pthreads was just emerging as a standard API and not
 * widely adopted yet.  The earliest MT implementation focused on SGI
 * Unix and provided multi- threading using multiple processses, and
 * shared memory.
 *
 * One of the relics of this model, namely the need to pre-allocate
 * locks and semaphores, and consequently a maximum number of them,
 * has been removed in the latest iteration of this layer.
 *
 */
/*
 * @- Mthreads Routine implementations
 */
#include "monetdb_config.h"
#include "gdk_system.h"

#ifdef TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# ifdef HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif

#ifndef HAVE_GETTIMEOFDAY
#ifdef HAVE_FTIME
#include <sys/timeb.h>
#endif
#endif

#ifdef HAVE_SIGNAL_H
# include <signal.h>
#endif

#include <unistd.h>		/* for sysconf symbols */

MT_Lock MT_system_lock
#ifdef PTHREAD_MUTEX_INITIALIZER
	= PTHREAD_MUTEX_INITIALIZER
#endif
	;

#ifdef MT_LOCK_TRACE
unsigned long long MT_locktrace_cnt[65536] = { 0 };
char *MT_locktrace_nme[65536] = { NULL };

#if defined(ia64) && !defined(__GNUC__)
#include "ia64regs.h"
#endif

unsigned long long
MT_clock()
{
	unsigned long long tsc;

#if defined(__x86_64__) && !defined(__PGI)
	unsigned long long a, d;
	__asm__ __volatile__("rdtsc":"=a"(a), "=d"(d));

	tsc = ((unsigned long) a) | (((unsigned long) d) << 32);
#define TICKNAME "rdtsc "
#elif defined(__i386__) && !defined(__PGI)
	__asm__ __volatile__("rdtsc":"=A"(tsc));

#define TICKNAME "rdtsc "
#elif !defined(ia64)
	tsc = (unsigned long long) clock();
#define TICKNAME "clock "
#elif defined(__GNUC__)
	__asm__ __volatile__("mov %0=ar.itc":"=r"(tsc)::"memory");

#define TICKNAME "ar.itc"
#else
	tsc = (unsigned long long) __getReg(_IA64_REG_AR_ITC);
#define TICKNAME "AR_ITC"
#endif /* defined(i386) */
	return tsc;
}
MT_Id MT_locktrace = 0;

void
MT_locktrace_start()
{
	memset(MT_locktrace_cnt, 0, 65536 * sizeof(unsigned long long));
	MT_locktrace = MT_getpid();
}

void
MT_locktrace_end()
{
	unsigned long long my_cnt[65536];
	memset(my_cnt, 0, 65536 * sizeof(unsigned long long));
	for (int i = 0; i < 65536; i++)
		if (MT_locktrace_cnt[i]) {
			int idx = MT_locktrace_hash(MT_locktrace_nme[i]);

			my_cnt[idx] += MT_locktrace_cnt[i];
		}
	for (int i = 0; i < 65536; i++)
		if (MT_locktrace_cnt[i]) {
			int idx = MT_locktrace_hash(MT_locktrace_nme[i]);

			if (my_cnt[idx])
				printf("%s " ULLFMT "\n", MT_locktrace_nme[i], my_cnt[idx]);
			my_cnt[idx] = 0;
		}
	MT_locktrace = 0;
}
#endif	/* MT_LOCK_TRACE */

#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
static struct winthread {
	struct winthread *next;
	HANDLE hdl;
	DWORD tid;
	void (*func) (void *);
	void *arg;
} *winthreads = NULL;
static CRITICAL_SECTION winthread_cs;
static int winthread_cs_init = 0;

static void
rm_winthread(struct winthread *w)
{
	struct winthread **wp;

	assert(winthread_cs_init);
	EnterCriticalSection(&winthread_cs);
	for (wp = &winthreads; *wp && *wp != w; wp = &(*wp)->next)
		;
	if (*wp)
		*wp = w->next;
	LeaveCriticalSection(&winthread_cs);
	free(w);
}

static DWORD WINAPI
thread_starter(LPVOID arg)
{
	(*((struct winthread *) arg)->func)(((struct winthread *) arg)->arg);
	if (((struct winthread *) arg)->hdl == NULL)
		rm_winthread((struct winthread *) arg);
	ExitThread(0);
	return TRUE;
}

int
MT_create_thread(MT_Id *t, void (*f) (void *), void *arg, enum MT_thr_detach d)
{
	struct winthread *w = malloc(sizeof(*w));

	if (winthread_cs_init == 0) {
		/* we only get here before any threads are created,
		 * and this is the only time that winthread_cs_init is
		 * ever changed */
		InitializeCriticalSection(&winthread_cs);
		winthread_cs_init = 1;
	}
	w->func = f;
	w->arg = arg;
	EnterCriticalSection(&winthread_cs);
	w->next = winthreads;
	winthreads = w;
	LeaveCriticalSection(&winthread_cs);
	w->hdl = CreateThread(NULL, THREAD_STACK_SIZE, thread_starter, w, 0, &w->tid);
	if (w->hdl == NULL) {
		rm_winthread(w);
		return -1;
	}
	*t = (MT_Id) w->tid;
	if (d == MT_THR_DETACHED) {
		/* not joinable */
		CloseHandle(w->hdl);
		w->hdl = NULL;
	}
	return 0;
}

void
MT_exit_thread(int s)
{
	DWORD t;
	struct winthread *w;

	if (winthread_cs_init) {
		t = GetCurrentThreadId();
		EnterCriticalSection(&winthread_cs);
		for (w = winthreads; w && w->tid != t; w = w->next)
			;
		LeaveCriticalSection(&winthread_cs);
		if (w->hdl == NULL)
			rm_winthread(w);
		ExitThread(s);
	} else {
		/* no threads started yet, so this is a global exit */
		MT_global_exit(s);
	}
}

int
MT_join_thread(MT_Id t)
{
	struct winthread *w;

	assert(winthread_cs_init);
	EnterCriticalSection(&winthread_cs);
	for (w = winthreads; w && w->tid != t; w = w->next)
		;
	LeaveCriticalSection(&winthread_cs);
	if (w == NULL || w->hdl == NULL)
		return -1;
	if (WaitForSingleObject(w->hdl, INFINITE) == WAIT_OBJECT_0 &&
	    CloseHandle(w->hdl)) {
		rm_winthread(w);
		return 0;
	}
	return -1;
}

int
MT_kill_thread(MT_Id t)
{
	struct winthread *w;

	assert(winthread_cs_init);
	EnterCriticalSection(&winthread_cs);
	for (w = winthreads; w && w->tid != t; w = w->next)
		;
	LeaveCriticalSection(&winthread_cs);
	if (w == NULL)
		return -1;
	if (w->hdl == NULL) {
		/* detached thread */
		HANDLE h;
		int ret = 0;
		h = OpenThread(THREAD_ALL_ACCESS, 0, (DWORD) t);
		if (h == NULL)
			return -1;
		if (TerminateThread(h, -1))
			ret = -1;
		CloseHandle(h);
		return ret;
	}
	if (TerminateThread(w->hdl, -1))
		return 0;
	return -1;
}

void
pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *mutexattr)
{
	(void) mutexattr;
	*mutex = CreateMutex(NULL, 0, NULL);
}

void
pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	CloseHandle(*mutex);
}

int
pthread_mutex_lock(pthread_mutex_t *mutex)
{
	return WaitForSingleObject(*mutex, INFINITE) == WAIT_OBJECT_0 ? 0 : -1;
}

int
pthread_mutex_trylock(pthread_mutex_t *mutex)
{
	return WaitForSingleObject(*mutex, 0) == WAIT_OBJECT_0 ? 0 : -1;
}

int
pthread_mutex_unlock(pthread_mutex_t *mutex)
{
	return ReleaseMutex(*mutex) ? 0 : -1;
}

void
pthread_sema_init(pthread_sema_t *s, int flag, int nresources)
{
	(void) flag;
	*s = CreateSemaphore(NULL, nresources, 0x7fffffff, NULL);
}

void
pthread_sema_destroy(pthread_sema_t *s)
{
	CloseHandle(*s);
}

void
pthread_sema_up(pthread_sema_t *s)
{
	ReleaseSemaphore(*s, 1, NULL);
}

void
pthread_sema_down(pthread_sema_t *s)
{
	WaitForSingleObject(*s, INFINITE);
}
int
pthread_cond_init(pthread_cond_t *cv, pthread_condattr_t *a)
{
	(void) a;
	cv->waiters_count = 0;
	cv->sema = CreateSemaphore(NULL, 0, 0x7fffffff, NULL);
	InitializeCriticalSection(&cv->waiters_count_lock);
	return 0;
}

int
pthread_cond_destroy(pthread_cond_t *cv)
{
	CloseHandle(cv->sema);
	return 0;
}

int
pthread_cond_signal(pthread_cond_t *cv)
{
	int have_waiters;

	EnterCriticalSection(&cv->waiters_count_lock);
	have_waiters = cv->waiters_count > 0;
	LeaveCriticalSection(&cv->waiters_count_lock);

	/* If there aren't any waiters, then this is a no-op. */
	if (have_waiters)
		ReleaseSemaphore(cv->sema, 1, 0);
	return 0;
}

int
pthread_cond_wait(pthread_cond_t *cv, pthread_mutex_t *external_mutex)
{
	EnterCriticalSection(&cv->waiters_count_lock);
	cv->waiters_count++;
	LeaveCriticalSection(&cv->waiters_count_lock);

	/* This call atomically releases the mutex and waits on the
	 * semaphore until <pthread_cond_signal> or
	 * <pthread_cond_broadcast> are called by another thread. */
	SignalObjectAndWait(*external_mutex, cv->sema, INFINITE, FALSE);

	EnterCriticalSection(&cv->waiters_count_lock);
	cv->waiters_count--;
	LeaveCriticalSection(&cv->waiters_count_lock);

	/* Always regain the external mutex since that's the guarantee
	 * we give to our callers. */
	WaitForSingleObject(*external_mutex, INFINITE);
	return 0;
}
#else  /* !defined(HAVE_PTHREAD_H) && defined(_MSC_VER) */
#ifdef HAVE_PTHREAD_SIGMASK
static void
MT_thread_sigmask(sigset_t * new_mask, sigset_t * orig_mask)
{
	(void) sigdelset(new_mask, SIGQUIT);
	(void) sigdelset(new_mask, SIGALRM);	/* else sleep doesn't work */
	(void) pthread_sigmask(SIG_SETMASK, new_mask, orig_mask);
}
#endif

int
MT_create_thread(MT_Id *t, void (*f) (void *), void *arg, enum MT_thr_detach d)
{
#ifdef HAVE_PTHREAD_SIGMASK
	sigset_t new_mask, orig_mask;
#endif
	pthread_attr_t attr;
	pthread_t newt;
	int ret;

#ifdef HAVE_PTHREAD_SIGMASK
	(void) sigfillset(&new_mask);
	MT_thread_sigmask(&new_mask, &orig_mask);
#endif
	pthread_attr_init(&attr);
	pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE);
	switch (d) {
		case MT_THR_JOINABLE:
			pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
		break;
		case MT_THR_DETACHED:
			pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
		break;
		default:
			assert(0);
	}
	ret = pthread_create(&newt, &attr, (void *(*)(void *)) f, arg);
	if (ret == 0)
#ifdef PTW32
		*t = (MT_Id) (((size_t) newt.p) + 1);	/* use pthread-id + 1 */
#else
		*t = (MT_Id) (((size_t) newt) + 1);	/* use pthread-id + 1 */
#endif
#ifdef HAVE_PTHREAD_SIGMASK
	MT_thread_sigmask(&orig_mask, NULL);
#endif
	return ret;
}

void
MT_exit_thread(int s)
{
	int st = s;

	pthread_exit(&st);
}

int
MT_join_thread(MT_Id t)
{
	pthread_t id;
#ifdef PTW32
	id.p = (void *) (t - 1);
	id.x = 0;
#else
	id = (pthread_t) (t - 1);
#endif
	return pthread_join(id, NULL);
}


int
MT_kill_thread(MT_Id t)
{
#ifdef HAVE_PTHREAD_KILL
	pthread_t id;
#ifdef PTW32
	id.p = (void *) (t - 1);
	id.x = 0;
#else
	id = (pthread_t) (t - 1);
#endif
	return pthread_kill(id, SIGHUP);
#else
	(void) t;
	return -1;		/* XXX */
#endif
}

#if defined(_AIX) || defined(__MACH__)
void
pthread_sema_init(pthread_sema_t *s, int flag, int nresources)
{
	(void) flag;
	s->cnt = nresources;
	pthread_mutex_init(&(s->mutex), 0);
	pthread_cond_init(&(s->cond), 0);
}

void
pthread_sema_destroy(pthread_sema_t *s)
{
	pthread_mutex_destroy(&(s->mutex));
	pthread_cond_destroy(&(s->cond));
}

void
pthread_sema_up(pthread_sema_t *s)
{
	(void)pthread_mutex_lock(&(s->mutex));

	if (s->cnt++ < 0) {
		/* wake up sleeping thread */
		(void)pthread_cond_signal(&(s->cond));
	}
	(void)pthread_mutex_unlock(&(s->mutex));
}

void
pthread_sema_down(pthread_sema_t *s)
{
	(void)pthread_mutex_lock(&(s->mutex));

	if (--s->cnt < 0) {
		/* thread goes to sleep */
		(void)pthread_cond_wait(&(s->cond), &(s->mutex));
	}
	(void)pthread_mutex_unlock(&(s->mutex));
}
#endif
#endif

#if !defined(WIN32) && defined(PROFILE) && defined(HAVE_PTHREAD_H)
#undef pthread_create
/* for profiling purposes (btw configure with --enable-profile *and*
 * --disable-shared --enable-static) without setting the ITIMER_PROF
 * per thread, all profiling info for everything except the main
 * thread is lost. */
#include <stdlib.h>

/* Our data structure passed to the wrapper */
typedef struct wrapper_s {
	void *(*start_routine) (void *);
	void *arg;

	pthread_mutex_t lock;
	pthread_cond_t wait;

	struct itimerval itimer;

} wrapper_t;

/* The wrapper function in charge for setting the itimer value */
static void *
wrapper_routine(void *data)
{
	/* Put user data in thread-local variables */
	void *(*start_routine) (void *) = ((wrapper_t *) data)->start_routine;
	void *arg = ((wrapper_t *) data)->arg;

	/* Set the profile timer value */
	setitimer(ITIMER_PROF, &((wrapper_t *) data)->itimer, NULL);

	/* Tell the calling thread that we don't need its data anymore */
	pthread_mutex_lock(&((wrapper_t *) data)->lock);

	pthread_cond_signal(&((wrapper_t *) data)->wait);
	pthread_mutex_unlock(&((wrapper_t *) data)->lock);

	/* Call the real function */
	return start_routine(arg);
}

/* Our wrapper function for the real pthread_create() */
int
gprof_pthread_create(pthread_t * __restrict thread, __const pthread_attr_t * __restrict attr, void *(*start_routine) (void *), void *__restrict arg)
{
	wrapper_t wrapper_data;
	int i_return;

	/* Initialize the wrapper structure */
	wrapper_data.start_routine = start_routine;
	wrapper_data.arg = arg;
	getitimer(ITIMER_PROF, &wrapper_data.itimer);
	pthread_cond_init(&wrapper_data.wait, NULL);
	pthread_mutex_init(&wrapper_data.lock, NULL);
	pthread_mutex_lock(&wrapper_data.lock);

	/* The real pthread_create call */
	i_return = pthread_create(thread, attr, &wrapper_routine, &wrapper_data);

	/* If the thread was successfully spawned, wait for the data
	 * to be released */
	if (i_return == 0) {
		pthread_cond_wait(&wrapper_data.wait, &wrapper_data.lock);
	}

	pthread_mutex_unlock(&wrapper_data.lock);
	pthread_mutex_destroy(&wrapper_data.lock);

	pthread_cond_destroy(&wrapper_data.wait);

	return i_return;
}
#endif

/* coverity[+kill] */
void
MT_global_exit(int s)
{
	exit(s);
}

MT_Id
MT_getpid(void)
{
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
	return (MT_Id) GetCurrentThreadId();
#elif defined(PTW32)
	return (MT_Id) (((size_t) pthread_self().p) + 1);
#else
	return (MT_Id) (((size_t) pthread_self()) + 1);
#endif
}

#define SMP_TOLERANCE 0.40
#define SMP_ROUNDS 1024*1024*128

static void
smp_thread(void *data)
{
	unsigned int s = 1, r;

	(void) data;
	for (r = 0; r < SMP_ROUNDS; r++)
		s = s * r + r;
	(void) s;
}

static int
MT_check_nr_cores_(void)
{
	int i, curr = 1, cores = 1;
	double lasttime = 0, thistime;
	while (1) {
		lng t0, t1;
		MT_Id *threads = malloc(sizeof(MT_Id) * curr);

		t0 = GDKusec();
		for (i = 0; i < curr; i++)
			MT_create_thread(threads + i, smp_thread, NULL, MT_THR_JOINABLE);
		for (i = 0; i < curr; i++)
			MT_join_thread(threads[i]);
		t1 = GDKusec();
		free(threads);
		thistime = (double) (t1 - t0) / 1000000;
		if (lasttime > 0 && thistime / lasttime > 1 + SMP_TOLERANCE)
			break;
		lasttime = thistime;
		cores = curr;
		curr *= 2;	/* only check for powers of 2 */
	}
	return cores;
}

int
MT_check_nr_cores(void)
{
	int ncpus = -1;

#if defined(HAVE_SYSCONF) && defined(_SC_NPROCESSORS_ONLN)
	/* this works on Linux, Solaris and AIX */
	ncpus = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(HAVE_SYS_SYSCTL_H) && defined(HW_NCPU)   /* BSD */
	size_t len = sizeof(int);
	int mib[3];

	/* Everyone should have permission to make this call,
	 * if we get a failure something is really wrong. */
	mib[0] = CTL_HW;
	mib[1] = HW_NCPU;
	mib[2] = -1;
	sysctl(mib, 3, &ncpus, &len, NULL, 0);
#elif defined(WIN32)
	SYSTEM_INFO sysinfo;

	GetSystemInfo(&sysinfo);
	ncpus = sysinfo.dwNumberOfProcessors;
#endif

	/* if we ever need HPUX or OSF/1 (hope not), see
	 * http://ndevilla.free.fr/threads/ */

	if (ncpus <= 0)
		ncpus = MT_check_nr_cores_();
#if SIZEOF_SIZE_T == SIZEOF_INT
	/* On 32-bits systems with large amounts of cpus/cores, we quickly
	 * run out of space due to the amount of threads in use.  Since it
	 * is questionable whether many cores on a 32-bits system are going
	 * to beneficial due to this, we simply limit the auto-detected
	 * cores to 16 on 32-bits systems.  The user can always override
	 * this via gdk_nr_threads. */
	if (ncpus > 16)
		ncpus = 16;
#endif

	return ncpus;
}



lng
GDKusec(void)
{
	/* Return the time in microseconds since an epoch.  The epoch
	 * is roughly the time this program started. */
#ifdef _MSC_VER
	static LARGE_INTEGER freq, start;	/* automatically initialized to 0 */
	LARGE_INTEGER ctr;

	if (start.QuadPart == 0 &&
	    (!QueryPerformanceFrequency(&freq) ||
	     !QueryPerformanceCounter(&start)))
		start.QuadPart = -1;
	if (start.QuadPart > 0) {
		QueryPerformanceCounter(&ctr);
		return (lng) (((ctr.QuadPart - start.QuadPart) * 1000000) / freq.QuadPart);
	}
#endif
#ifdef HAVE_GETTIMEOFDAY
	{
		static struct timeval tpbase;	/* automatically initialized to 0 */
		struct timeval tp;

		if (tpbase.tv_sec == 0)
			gettimeofday(&tpbase, NULL);
		gettimeofday(&tp, NULL);
		tp.tv_sec -= tpbase.tv_sec;
		return (lng) tp.tv_sec * 1000000 + (lng) tp.tv_usec;
	}
#else
#ifdef HAVE_FTIME
	{
		static struct timeb tbbase;	/* automatically initialized to 0 */
		struct timeb tb;

		if (tbbase.time == 0)
			ftime(&tbbase);
		ftime(&tb);
		tb.time -= tbbase.time;
		return (lng) tb.time * 1000000 + (lng) tb.millitm * 1000;
	}
#endif
#endif
}


int
GDKms(void)
{
	return (int) (GDKusec() / 1000);
}
