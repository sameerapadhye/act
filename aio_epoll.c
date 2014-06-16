/*
   Asynchronous reads for the ACT.
   Async IO with an epoll loop.  	
 */

#include <sys/types.h>
#include <signal.h>
#include <sys/timerfd.h>
#include <dirent.h>
#include <execinfo.h>	// for debugging
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>		
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <linux/fs.h>
#include <openssl/rand.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/eventfd.h>
#include <aio.h>
#include <sys/epoll.h>
#include <errno.h>

#include "atomic.h"
#include "clock.h"
#include "histogram.h"
#include "queue.h"

#include <linux/aio_abi.h>
#include <sys/syscall.h>

/*********************************************************************************************
  CONSTANTS
 *********************************************************************************************/
const char TAG_DEVICE_NAMES[]			= "device-names";
const char TAG_NUM_TIMERS[]			= "num-timers";
const char TAG_WORKER_THREADS[]			= "worker-threads";
const char TAG_RUN_SEC[]			= "test-duration-sec";
const char TAG_REPORT_INTERVAL_SEC[]		= "report-interval-sec";
const char TAG_READ_REQS_PER_SEC[]		= "read-reqs-per-sec";
const char TAG_LARGE_BLOCK_OPS_PER_SEC[]	= "large-block-ops-per-sec";
const char TAG_READ_REQ_NUM_512_BLOCKS[]	= "read-req-num-512-blocks";
const char TAG_LARGE_BLOCK_OP_KBYTES[]		= "large-block-op-kbytes";
const char TAG_USE_VALLOC[]			= "use-valloc";
const char TAG_NUM_WRITE_BUFFERS[]		= "num-write-buffers";
const char TAG_SCHEDULER_MODE[]			= "scheduler-mode";

#define MAX_NUM_DEVICES 32
#define MAX_DEVICE_NAME_SIZE 64
#define WHITE_SPACE " \t\n\r"

const uint32_t MIN_BLOCK_BYTES = 512;
const uint32_t RAND_SEED_SIZE = 64;
const uint64_t MAX_READ_REQS_QUEUED = 100000;

const char* const SCHEDULER_MODES[] = {
	"noop",
	"cfq"
};

const uint32_t NUM_SCHEDULER_MODES = sizeof(SCHEDULER_MODES) / sizeof(char*);

// Linux has removed O_DIRECT, but not its functionality.
#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

#define MAXEVENTS 5000 /* TODO this is actually max events per worker thread */
#define AS_ASYNC_READ 1
#define AS_TIMER 2
#define MAX_EVENTFDS 5000


/*********************************************************************************************
  STRUCTURES 
 *********************************************************************************************/
typedef struct _device {
	const char* name;
	uint64_t num_512_blocks;
	uint64_t num_large_blocks;
	int fd;
	pthread_t large_block_read_thread;
	pthread_t large_block_write_thread;
	uint8_t* p_large_block_read_buffer;
	histogram* p_raw_read_histogram;
	char histogram_tag[MAX_DEVICE_NAME_SIZE];
} device;

typedef struct _readreq {
	device* p_device;
	uint64_t offset;
	uint32_t size;
	uint64_t start_time;
} readreq;

typedef struct _readq {
	cf_queue* p_req_queue;
	pthread_t* threads;
} readq;

typedef struct _salter {
	uint8_t* p_buffer;
	pthread_mutex_t lock;
	uint32_t stamp;
} salter;

typedef struct 
{
	uint64_t raw_start_time; 
	uint8_t *p_buffer;
	readreq p_readreq;
	struct iocb aio_cb;
	struct epoll_event event;
	int efd;
	int fd;  
} as_async_info_t;


/*********************************************************************************************
  GLOBALS
 *********************************************************************************************/
static uint8_t g_rand_64_buffer[1024 * 8];
static size_t g_rand_64_buffer_offset = 0;
static pthread_mutex_t g_rand_64_lock = PTHREAD_MUTEX_INITIALIZER;

static char g_device_names[MAX_NUM_DEVICES][MAX_DEVICE_NAME_SIZE];
static uint32_t g_num_devices = 0;
static uint32_t g_worker_threads = 4; //TODO default? 
static uint64_t g_run_ms = 0;
static uint32_t g_report_interval_ms = 0;
static uint64_t g_read_reqs_per_sec = 0;
static uint64_t g_large_block_ops_per_sec = 0;
static uint32_t g_read_req_num_512_blocks = 0;
static uint32_t g_large_block_ops_bytes = 0;
static bool g_use_valloc = false;
static uint32_t g_num_write_buffers = 0;
static uint32_t g_scheduler_mode = 0;

static salter* g_salters;
static device* g_devices;

static uint32_t g_running;
static uint64_t g_run_start_ms;

static cf_atomic_int g_read_reqs_queued = 0;

static histogram* g_p_large_block_read_histogram;
static histogram* g_p_large_block_write_histogram;
static histogram* g_p_raw_read_histogram;
static histogram* g_p_read_histogram;

static cf_queue* async_info_queue;/* Queue of all the as_async_info_t pointers that are mallocd */ 
static as_async_info_t *async_info_array;/* Actual mallocd as_async_info_t structs */

static cf_queue* p_buffer_queue;/* Queue of all the p_buffer pointers inside 
				   the as_async_info_t struct that are cf_vallocd */ 

int async_epfd;

//static cf_queue *eventfd_queue; /* Queue of eventfds recycled and used throughout the program */
int eventfd_array[MAX_EVENTFDS + 10];/* Buffer of 10 for other fd s in the program that might have 
					been created before these
					(Since array index here is the efd no) */
int start_efd, current_efd, last_efd;/* To manage the eventfd array */
pthread_mutex_t efd_lock;/* Lock to access the shared eventfd array */

/*********************************************************************************************
  FORWARD DECLARATION
 *********************************************************************************************/

static void*	run_large_block_reads(void* pv_device);
static void*	run_large_block_writes(void* pv_device);
static uint64_t	read_from_device(device* p_device, uint64_t offset,
		uint32_t size, uint8_t* p_buffer);
static int read_async_from_device(as_async_info_t *info, aio_context_t aio_context);
static inline uint8_t* align_4096(uint8_t* stack_buffer);
static inline uint8_t* cf_valloc(size_t size);
static bool		check_config();
static void		config_parse_device_names();
static void		config_parse_scheduler_mode();
static uint32_t	config_parse_uint32();
static bool		config_parse_yes_no();
static bool		configure(int argc, char* argv[]);
static bool		create_large_block_read_buffer(device* p_device);
static bool		create_salters();
static void		destroy_salters();
static void		discover_num_blocks(device* p_device);
static inline uint32_t rand_32();
static uint64_t	rand_64();
static bool		rand_fill(uint8_t* p_buffer, uint32_t size);
static bool		rand_seed(uint8_t* p_buffer);
static uint64_t	random_read_offset(device* p_device);
static uint64_t	random_large_block_offset(device* p_device);
static void		read_and_report_large_block(device* p_device);
static inline uint64_t safe_delta_ms(uint64_t start_ms, uint64_t stop_ms);
static void		set_schedulers();
static void		write_and_report_large_block(device* p_device);
static uint64_t	write_to_device(device* p_device, uint64_t offset,
		uint32_t size, uint8_t* p_buffer);

static void		as_sig_handle_segv(int sig_num);
static void		as_sig_handle_term(int sig_num);

static int io_setup(unsigned nr, aio_context_t *ctxp);
static int io_destroy(aio_context_t ctx);
static int io_submit(aio_context_t ctx, long nr,  struct iocb **iocbpp); 
static int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
		struct io_event *events, struct timespec *timeout);
static void io_set_eventfd(struct iocb *iocb, int eventfd);

static int get_efd();
static void put_efd(int efd);

/*********************************************************************************************
 *********************************************************************************************/

static int io_setup(unsigned nr, aio_context_t *ctxp)
{
	return syscall(__NR_io_setup, nr, ctxp); 
}

static int io_destroy(aio_context_t ctx) 
{
	return syscall(__NR_io_destroy, ctx);
}

static int io_submit(aio_context_t ctx, long nr,  struct iocb **iocbpp) 
{
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

static int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
		struct io_event *events, struct timespec *timeout)
{
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

/* Will ensure that the eventfd is wrtten to at the end of the async read */
static void io_set_eventfd (struct iocb *iocb, int eventfd)
{
	iocb->aio_flags |= (1 << 0); /* IOCB_FLAG_RESFD */
	iocb->aio_resfd = eventfd;
}

/* Setting up the read request, registering the eventfd on epoll */
int aio_read_setup(struct iocb *cb, int fd, as_async_info_t *info)
{
	memset(cb, 0, sizeof(struct iocb));
	cb->aio_fildes = fd;
	cb->aio_lio_opcode = IOCB_CMD_PREAD;

	/* Read parameters */
	cb->aio_offset = info->p_readreq.offset;
	cb->aio_buf = (uint64_t)info->p_buffer;
	cb->aio_nbytes = info->p_readreq.size;
	/* Info structure */
	cb->aio_data = (uint64_t)info;

	int efd = get_efd();
	io_set_eventfd(cb, efd);
	info->efd = efd;

	struct epoll_event *epev = &(info->event);
	epev->events = EPOLLIN | EPOLLONESHOT;
	epev->data.ptr = info;
	if(epoll_ctl(async_epfd, EPOLL_CTL_MOD, efd, epev) < 0)
	{
		fprintf(stdout,"Error: epoll ctl failed \n");
		return -1;
	}
	return 0; 
}

static int read_async_from_device(as_async_info_t *info, aio_context_t aio_ctx) 
{
	int fd = info->p_readreq.p_device->fd;
	info->fd = fd;
	if (fd == -1) 
	{
		return -1;
	}

	struct iocb *aio_cb = &info->aio_cb;
	if(aio_read_setup(aio_cb, fd, info)!= 0)
		return -1;

	int ret = io_submit(aio_ctx, 1 , &aio_cb);

	if(ret != 1)
	{
		if(ret < 0)
		{
			fprintf(stdout, "Error: io_submit failed \n");
		}
		else 
		{
			fprintf(stdout,"Error: Failed to submit IO \n");
		}
		return -1;
	} 
	return 0;
}

static void create_p_buffer_queue()
{
	int i;
	uintptr_t buf;
	uint8_t *temp_buf;
	p_buffer_queue = cf_queue_create(sizeof(uintptr_t), true);
	int size = g_read_req_num_512_blocks * MIN_BLOCK_BYTES;

	for(i = 0; i < MAX_READ_REQS_QUEUED; i++)
	{
		temp_buf = cf_valloc(size);
		if(temp_buf == NULL)
		{
			fprintf(stdout,"Error: cf_valloc failed \n");
			cf_queue_destroy(p_buffer_queue);
			exit(-1);
		}
		buf = (uintptr_t)temp_buf;
		cf_queue_push(p_buffer_queue, (void*)&buf);
	}
}

static void destroy_p_buffer_queue()
{
	int i;
	uintptr_t ptr;
	for(i = 0; i < MAX_READ_REQS_QUEUED; i++)
	{
		if(cf_queue_pop(p_buffer_queue, (void*)&ptr, CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY)
		{
			goto destroy;
		}
		free((uint8_t*)ptr);
	}
destroy:
	cf_queue_destroy(p_buffer_queue);
}

/* Function called by dedicated thread to generate read requests ar regular intervals */
static void* generate_async_reads(void* aio_context)
{
	uint64_t count = 0;
	while(g_running)
	{
		/* Create the struct of info needed at the process_read end */
		uintptr_t info_ptr;
		if (cf_queue_pop(async_info_queue, (void*)&info_ptr, CF_QUEUE_NOWAIT) !=
				CF_QUEUE_OK) 
		{
			fprintf(stdout, "Error: Could not pop info struct \n");
			goto fail;
		}
		/* Get p_buffer from queue */
		uintptr_t p_buf_ptr = -1;
		if (cf_queue_pop(p_buffer_queue, (void*)&p_buf_ptr, CF_QUEUE_NOWAIT) !=
				CF_QUEUE_OK) 
		{
			fprintf(stdout, "Error: Could not pop p_buffer ptr \n");
			goto fail;
		}

		as_async_info_t *info = (as_async_info_t*)info_ptr;
		uint8_t *p_buffer = (uint8_t*)p_buf_ptr;	

		memset(info, 0, sizeof(as_async_info_t));
		/* Generate the actual read request */
		uint32_t random_device_index = rand_32() % g_num_devices;
		device* p_random_device = &g_devices[random_device_index];
		readreq* p_readreq = &(info->p_readreq);
		if(p_readreq == NULL)
		{
			fprintf(stdout, "Error: preadreq null \n");
			goto fail;
		}
		p_readreq->p_device = p_random_device;
		p_readreq->offset = random_read_offset(p_random_device);
		p_readreq->size = g_read_req_num_512_blocks * MIN_BLOCK_BYTES;
		p_readreq->start_time = cf_getms();

		/* Async read */
		info->p_buffer = p_buffer;
		if (p_buffer) 
		{
			uint64_t raw_start_time = cf_getms();
			info->raw_start_time = raw_start_time;
			if(read_async_from_device(info, *(aio_context_t *)aio_context) < 0)
			{
				fprintf(stdout, "Error: Async read failed \n");
				free(p_buffer);
				goto fail; 
			}
		}
		else 
		{
			fprintf(stdout, "ERROR: read buffer cf_valloc()\n");
		}
		if (cf_atomic_int_incr(&g_read_reqs_queued) > MAX_READ_REQS_QUEUED) 
		{
			fprintf(stdout, "ERROR: too many read reqs queued\n");
			fprintf(stdout, "drive(s) can't keep up - test stopped\n");
			g_running = false;
			return (void*)-1;;
		}

		count++;

		int sleep_ms = (int)
			(((count * 1000) / g_read_reqs_per_sec) -
			 (cf_getms() - g_run_start_ms));

		if (sleep_ms > 0) {
			usleep((uint32_t)sleep_ms * 1000);
		}

		continue;

		/* Rollback for failure */
fail:
		if(info_ptr != -1)
			cf_queue_push(async_info_queue, (void*)&info_ptr);	

		if(p_buf_ptr != -1)
			cf_queue_push(p_buffer_queue, (void*)&p_buf_ptr);

	}
	return (0);
}

/* Processing reads when they return from aio_read */
static void process_read(as_async_info_t *info)
{ 
	if(!g_running)
	{
		return;
	}
	cf_atomic_int_decr(&g_read_reqs_queued);
	uint64_t stop_time = cf_getms();

	if (stop_time != -1) 
	{
		histogram_insert_data_point(g_p_raw_read_histogram,
				safe_delta_ms(info->raw_start_time, stop_time));
		histogram_insert_data_point(g_p_read_histogram,
				safe_delta_ms(info->p_readreq.start_time, stop_time));
		histogram_insert_data_point(
				info->p_readreq.p_device->p_raw_read_histogram,
				safe_delta_ms(info->raw_start_time, stop_time));
	}
	if (g_use_valloc && info->p_buffer) 
	{
		free(info->p_buffer);
	}

	uintptr_t temp_buf = (uintptr_t)info->p_buffer;
	cf_queue_push(p_buffer_queue, (void*)&temp_buf);

	uintptr_t temp = (uintptr_t)info;
	cf_queue_push(async_info_queue, (void*)&temp);
}

/* Called by each worker thread */
static void* worker_func(void* aio_context)
{
	int num_events = -1, i = 0;
	as_async_info_t *reference;
	struct io_event events[MAXEVENTS];
	uint64_t value;
	struct epoll_event *epoll_events = (struct epoll_event*)calloc(MAXEVENTS , sizeof(struct epoll_event)); 
	while(g_running)
	{

		num_events = epoll_wait(async_epfd, epoll_events, MAXEVENTS, -1);
		if(num_events < 0)
		{
			continue;
		}	
		num_events = -1;
		num_events = io_getevents(*(aio_context_t*)aio_context, 1, MAXEVENTS, (events + 0), NULL);

		for(i = 0; i < num_events; i++)
		{
			reference = (as_async_info_t*)events[i].data;
			/* Resetting the eventfd and returning to its queue for another read request to use */
			read(reference->efd, &value, sizeof(uint64_t));
			put_efd(reference->efd);
			process_read(reference);  
		}
	}
	return (0);	
}

/* Queue for as_async_info_t structures 
   First malloc a block of memory and then split into structures and
   store pointers in a queue */
static void create_async_info_queue()
{
	int i;
	uintptr_t info;
	as_async_info_t *temp_info;
	async_info_queue = cf_queue_create(sizeof(uintptr_t), true);

	async_info_array = (as_async_info_t*)malloc(MAX_READ_REQS_QUEUED * sizeof(as_async_info_t));
	if(async_info_array == NULL)
	{
		fprintf(stdout, "Error: Malloc info structs failed.\n Exiting. \n");
		cf_queue_destroy(async_info_queue);
		exit(-1);
	}
	for(i = 0; i < MAX_READ_REQS_QUEUED; i++)
	{
		temp_info = async_info_array + i;
		info = (uintptr_t)temp_info;
		cf_queue_push(async_info_queue, (void*)&info);
	}
}

static void destroy_async_info_queue()
{
	free(async_info_array);
	cf_queue_destroy(async_info_queue);
}


static void create_eventfd_array()
{

	pthread_mutex_init(&efd_lock, NULL);
	int i, efd = -1;	
	start_efd = -2;
	efd = eventfd(0,EFD_NONBLOCK); 
	start_efd = efd;
	current_efd = start_efd;

	for(i = 0; i < start_efd; i++)
	{
		eventfd_array[i] = 0;
	}
	for(i = 0; i < MAX_EVENTFDS; i++)
	{	
		if (i != 0)
			efd = eventfd(0,EFD_NONBLOCK); 

		eventfd_array[efd] = 1;	
		struct epoll_event event;
		event.events = EPOLLIN | EPOLLONESHOT;	

		if(epoll_ctl(async_epfd, EPOLL_CTL_ADD, efd, &event) < 0)
		{
			fprintf(stdout,"Error: epoll ctl failed \n");
			exit(-1);
		} 
		if(i == MAX_EVENTFDS -1)
		{
			last_efd = efd;
		}
	}

}

static int get_efd()
{
	pthread_mutex_lock(&efd_lock);
	int start = current_efd;

	while(1)
	{
		if(eventfd_array[current_efd] == 1)
		{
			eventfd_array[current_efd] = 0;
			pthread_mutex_unlock(&efd_lock);
			return current_efd;
		}	
		current_efd++;
		if(current_efd >= last_efd)
		{
			current_efd = start_efd;
		} 
		if(current_efd == start)
		{
			fprintf(stdout, "No more available eventfds\n");
			pthread_mutex_unlock(&efd_lock);
			exit(-1);
		}
		//OR can keep a count and check if = max eventfds TODO
	}
}

static void put_efd(int efd)
{
	pthread_mutex_lock(&efd_lock);
	if(eventfd_array[efd] != 0)
	{
		fprintf(stdout, "Error: Eventfd array corrupted\n");
		pthread_mutex_unlock(&efd_lock);
		exit(-1);
	}
	eventfd_array[efd] = 1;
	pthread_mutex_unlock(&efd_lock);
}
/*	
	static void create_eventfd_queue()
	{
	int i, efd, async_epfd;
	eventfd_queue = cf_queue_create(sizeof(int), true);
	for(i = 0; i < MAX_EVENTFDS; i++)
	{
	efd = eventfd(0,EFD_NONBLOCK); 
	cf_queue_push(eventfd_queue, (void*)&efd);

	struct epoll_event event;
	event.events = EPOLLIN | EPOLLONESHOT;	

	async_epfd = epoll_fd[i % g_worker_threads];

	if(epoll_ctl(async_epfd, EPOLL_CTL_ADD, efd, &event) < 0)
	{
	fprintf(stdout,"Error: epoll ctl failed \n");
	exit(-1);
	} 
	}
	}

 */
static void destroy_eventfd_array()
{
	pthread_mutex_destroy(&efd_lock);
	int i, efd;		
	for(i = 0; i < MAX_EVENTFDS; i++)
	{
		efd = start_efd + i;
		if(efd != -1)
			close(efd);
	}
}
/*
   static void destroy_eventfd_queue()
   {
   int i, efd;		
   for(i = 0; i < MAX_EVENTFDS; i++)
   {
   cf_queue_pop(eventfd_queue, (void*)&efd, CF_QUEUE_NOWAIT);
   if(efd != -1)
   close(efd);
   }
   cf_queue_destroy(eventfd_queue);
   }
 */

int main(int argc, char* argv[]) {
	signal(SIGSEGV, as_sig_handle_segv);
	signal(SIGTERM, as_sig_handle_term);


	fprintf(stdout, "\nAerospike act - device IO test\n");
	fprintf(stdout, "Copyright 2011 by Aerospike. All rights reserved.\n\n");

	if (! configure(argc, argv)) {
		exit(-1);
	}

	set_schedulers();
	srand(time(NULL));
	//	rand_seed(g_rand_64_buffer);

	salter salters[g_num_write_buffers ? g_num_write_buffers : 1];

	g_salters = salters;

	if (! create_salters()) {
		exit(-1);
	}

	device devices[g_num_devices];
	g_devices = devices;

	g_p_large_block_read_histogram = histogram_create();
	g_p_large_block_write_histogram = histogram_create();
	g_p_raw_read_histogram = histogram_create();
	g_p_read_histogram = histogram_create();

	g_run_start_ms = cf_getms();

	uint64_t run_stop_ms = g_run_start_ms + g_run_ms;

	g_running = 1;
	int n, fd = -1;
	for (n = 0; n < g_num_devices; n++) 
	{
		device* p_device = &g_devices[n];
		p_device->name = g_device_names[n];

		fd = open(p_device->name, O_DIRECT | O_RDWR, S_IRUSR | S_IWUSR);
		if (fd == -1) 
		{
			fprintf(stdout, "ERROR: open device %s \n", p_device->name);
			exit(-1);
		}
		p_device->fd = fd;

		discover_num_blocks(p_device);
		create_large_block_read_buffer(p_device);
		p_device->p_raw_read_histogram = histogram_create();
		sprintf(p_device->histogram_tag, "%-18s", p_device->name);

		if (pthread_create(&p_device->large_block_read_thread, NULL,
					run_large_block_reads, (void*)p_device)) 
		{
			fprintf(stdout, "Error: create large block read thread %d\n", n);
			exit(-1);
		}

		if (pthread_create(&p_device->large_block_write_thread, NULL,
					run_large_block_writes, (void*)p_device)) 
		{
			fprintf(stdout, "Error: create write thread %d\n", n);
			exit(-1);
		}

	}

	/* Epoll */
	async_epfd = epoll_create1(0);
	if(async_epfd < 0)
	{
		fprintf(stdout, "Error: Async epfd not created. Exiting. \n");
		exit(-1);
	} 

	create_p_buffer_queue();
	create_eventfd_array();

	/* AIO */
	aio_context_t aio_context = 0;
	if(io_setup(MAXEVENTS, &aio_context) != 0)
	{
		fprintf(stdout, "Error: AIO context not set up \n");
		exit(-1);
	}
	create_async_info_queue();

	/* Dedicated thread to generate read events */
	pthread_t read_generator;
	if (pthread_create(&read_generator, NULL, &generate_async_reads, (void*)&aio_context)) 
	{
		fprintf(stdout, "Error: create read generator thread\n");
		exit(-1);
	}

	/* Worker threads */
	pthread_t workers[g_worker_threads];
	int j;
	for (j = 0; j < g_worker_threads; j++) 
	{ 
		if (pthread_create(&workers[j], NULL, &worker_func , (void *)(&aio_context))) 
		{
			fprintf(stdout, "Error: creating worker thread %d failed\n", j);
			exit(-1);
		}	
	}

	fprintf(stdout, "\n");
	uint64_t now_ms;
	uint64_t time_count = 0;
	int nanosleep_ret = -1;
	struct timespec initial,remaining;
	while ((now_ms = cf_getms()) < run_stop_ms && g_running) 
	{	
		time_count++;
		int sleep_ms = (int)
			((time_count * g_report_interval_ms) - (now_ms - g_run_start_ms));
		if (sleep_ms > 0) 
		{
			initial.tv_sec = sleep_ms / 1000;
			initial.tv_nsec = (sleep_ms % 1000) * 1000000;
retry:
			memset(&remaining, 0, sizeof(remaining));
			nanosleep_ret = nanosleep(&initial, &remaining);
			if(nanosleep_ret == -1 && errno == EINTR)
			{
				/* Interrupted by a signal */
				initial.tv_sec = remaining.tv_sec;
				initial.tv_nsec = remaining.tv_nsec;	
				goto retry;	
			}
		}

		fprintf(stdout, "After %" PRIu64 " sec:\n",
				(time_count * g_report_interval_ms) / 1000);

		fprintf(stdout, "read-reqs queued: %" PRIu64 "\n",
				cf_atomic_int_get(g_read_reqs_queued));

		histogram_dump(g_p_large_block_read_histogram,  "LARGE BLOCK READS ");
		histogram_dump(g_p_large_block_write_histogram, "LARGE BLOCK WRITES");
		histogram_dump(g_p_raw_read_histogram,          "RAW READS         ");
		int d;
		for (d = 0; d < g_num_devices; d++) {			
			histogram_dump(g_devices[d].p_raw_read_histogram,
					g_devices[d].histogram_tag);	
		}

		histogram_dump(g_p_read_histogram,              "READS             ");

		fprintf(stdout, "\n");
		fflush(stdout);
	}
	fprintf(stdout, "\nTEST COMPLETED \n");
	g_running = 0;
	int i;
	//TODO aio_destroy? Blocks on incomplete requests? 

	/* Freeing resources used by async */
	void* ret_value;
	for (i = 0; i < g_worker_threads; i++) 
	{
		pthread_join(workers[i], &ret_value);
	}

	destroy_eventfd_array();
	destroy_p_buffer_queue();
	destroy_async_info_queue();

	int d;
	for (d = 0; d < g_num_devices; d++) {
		device* p_device = &g_devices[d];

		pthread_join(p_device->large_block_read_thread, &ret_value);
		pthread_join(p_device->large_block_write_thread, &ret_value);

		close(p_device->fd);

		free(p_device->p_large_block_read_buffer);
		free(p_device->p_raw_read_histogram);
	}

	free(g_p_large_block_read_histogram);
	free(g_p_large_block_write_histogram);
	free(g_p_raw_read_histogram);
	free(g_p_read_histogram);

	destroy_salters();

	return (0);
}



//------------------------------------------------
// Runs in every device large-block read thread,
// executes large-block reads at a constant rate.
//
static void* run_large_block_reads(void* pv_device) {
	device* p_device = (device*)pv_device;
	uint64_t count = 0;

	while (g_running) {
		read_and_report_large_block(p_device);

		count++;

		int sleep_ms = (int)
			(((count * 1000 * g_num_devices) / g_large_block_ops_per_sec) -
			 (cf_getms() - g_run_start_ms));

		if (sleep_ms > 0) {
			usleep((uint32_t)sleep_ms * 1000);
		}

	}

	return (0);
}

//------------------------------------------------
// Runs in every device large-block write thread,
// executes large-block writes at a constant rate.
//
static void* run_large_block_writes(void* pv_device) {
	device* p_device = (device*)pv_device;
	uint64_t count = 0;

	while (g_running) {
		write_and_report_large_block(p_device);

		count++;

		int sleep_ms = (int)
			(((count * 1000 * g_num_devices) / g_large_block_ops_per_sec) -
			 (cf_getms() - g_run_start_ms));

		if (sleep_ms > 0) {
			usleep((uint32_t)sleep_ms * 1000);
		}

	}

	return (0);
}


//==========================================================
// Helpers
//

//------------------------------------------------
// Align stack-allocated memory.
//
static inline uint8_t* align_4096(uint8_t* stack_buffer) {
	return (uint8_t*)(((uint64_t)stack_buffer + 4095) & ~4095ULL);
}

//------------------------------------------------
// Aligned memory allocation.
//
static inline uint8_t* cf_valloc(size_t size) {
	void* pv;

	return posix_memalign(&pv, 4096, size) == 0 ? (uint8_t*)pv : 0;
}

//------------------------------------------------
// Check (and finish setting) run parameters.
//
static bool check_config() {
	fprintf(stdout, "CIO CONFIGURATION\n");

	fprintf(stdout, "%s:", TAG_DEVICE_NAMES);
	int d;
	for (d = 0; d < g_num_devices; d++) {
		fprintf(stdout, " %s", g_device_names[d]);
	}

	fprintf(stdout, "\nnum-devices: %" PRIu32 "\n",
			g_num_devices);
	fprintf(stdout, "%s: %" PRIu64 "\n",	TAG_RUN_SEC,
			g_run_ms / 1000);
	fprintf(stdout, "%s: %" PRIu32 "\n",	TAG_REPORT_INTERVAL_SEC,
			g_report_interval_ms / 1000);
	fprintf(stdout, "%s: %" PRIu64 "\n",	TAG_READ_REQS_PER_SEC,
			g_read_reqs_per_sec);
	fprintf(stdout, "%s: %" PRIu64 "\n",	TAG_LARGE_BLOCK_OPS_PER_SEC,
			g_large_block_ops_per_sec);
	fprintf(stdout, "%s: %" PRIu32 "\n",	TAG_READ_REQ_NUM_512_BLOCKS,
			g_read_req_num_512_blocks);
	fprintf(stdout, "%s: %" PRIu32 "\n",	TAG_LARGE_BLOCK_OP_KBYTES,
			g_large_block_ops_bytes / 1024);
	fprintf(stdout, "%s: %s\n",				TAG_USE_VALLOC,
			g_use_valloc ? "yes" : "no");
	fprintf(stdout, "%s: %" PRIu32 "\n",	TAG_NUM_WRITE_BUFFERS,
			g_num_write_buffers);
	fprintf(stdout, "%s: %s\n",				TAG_SCHEDULER_MODE,
			SCHEDULER_MODES[g_scheduler_mode]);
	fprintf(stdout, "\n");

	if (! (	g_num_devices &&
				g_worker_threads &&
				g_run_ms &&
				g_report_interval_ms &&
				g_read_reqs_per_sec &&
				g_large_block_ops_per_sec &&
				g_read_req_num_512_blocks &&
				g_large_block_ops_bytes)) {
		fprintf(stdout, "ERROR: invalid configuration\n");
		return false;
	}

	return true;
}

//------------------------------------------------
// Parse device names parameter.
//
static void config_parse_device_names() {
	const char* val;

	while ((val = strtok(NULL, ",;" WHITE_SPACE)) != NULL) {
		int name_length = strlen(val);

		if (name_length == 0 || name_length >= MAX_DEVICE_NAME_SIZE) {
			continue;
		}

		strcpy(g_device_names[g_num_devices], val);
		g_num_devices++;

		if (g_num_devices >= MAX_NUM_DEVICES) {
			break;
		}
	}
}

//------------------------------------------------
// Parse system block scheduler mode.
//
static void config_parse_scheduler_mode() {
	const char* val = strtok(NULL, WHITE_SPACE);

	if (! val) {
		return;
	}
	uint32_t m;
	for (m = 0; m < NUM_SCHEDULER_MODES; m++) {
		if (! strcmp(val, SCHEDULER_MODES[m])) {
			g_scheduler_mode = m;
		}
	}
}

//------------------------------------------------
// Parse numeric run parameter.
//
static uint32_t config_parse_uint32() {
	const char* val = strtok(NULL, WHITE_SPACE);

	return val ? strtoul(val, NULL, 10) : 0;
}

//------------------------------------------------
// Parse yes/no run parameter.
//
static bool config_parse_yes_no() {
	const char* val = strtok(NULL, WHITE_SPACE);

	return val && *val == 'y';
}

//------------------------------------------------
// Set run parameters.
//
static bool configure(int argc, char* argv[]) {
	if (argc != 2) {
		fprintf(stdout, "usage: act [config filename]\n");
		return false;
	}

	FILE* config_file = fopen(argv[1], "r");

	if (! config_file) {
		fprintf(stdout, "couldn't open config file: %s\n", argv[1]);
		return false;
	}

	char line[1024];

	while (fgets(line, sizeof(line), config_file)) {
		if (*line == '#') {
			continue;
		}

		const char* tag = strtok(line, ":" WHITE_SPACE);

		if (! tag) {
			continue;
		}

		if(! strcmp(tag, TAG_DEVICE_NAMES)) {
			config_parse_device_names();
		}
		else if (! strcmp(tag, TAG_RUN_SEC)) {
			g_run_ms = (uint64_t)config_parse_uint32() * 1000;
		}
		else if (! strcmp(tag, TAG_REPORT_INTERVAL_SEC)) {
			g_report_interval_ms = config_parse_uint32() * 1000;
		}
		else if (! strcmp(tag, TAG_READ_REQS_PER_SEC)) {
			g_read_reqs_per_sec = (uint64_t)config_parse_uint32();
		}
		else if (! strcmp(tag, TAG_LARGE_BLOCK_OPS_PER_SEC)) {
			g_large_block_ops_per_sec = (uint64_t)config_parse_uint32();
		}
		else if (! strcmp(tag, TAG_READ_REQ_NUM_512_BLOCKS)) {
			g_read_req_num_512_blocks = config_parse_uint32();
		}
		else if (! strcmp(tag, TAG_LARGE_BLOCK_OP_KBYTES)) {
			g_large_block_ops_bytes = config_parse_uint32() * 1024;
		}
		else if (! strcmp(tag, TAG_USE_VALLOC)) {
			g_use_valloc = config_parse_yes_no();
		}
		else if (! strcmp(tag, TAG_NUM_WRITE_BUFFERS)) {
			g_num_write_buffers = config_parse_uint32();
		}
		else if (! strcmp(tag, TAG_SCHEDULER_MODE)) {
			config_parse_scheduler_mode();
		}
	}

	fclose(config_file);

	return check_config();
}

//------------------------------------------------
// Create device large block read buffer.
//
static bool create_large_block_read_buffer(device* p_device) {
	if (! (p_device->p_large_block_read_buffer =
				cf_valloc(g_large_block_ops_bytes))) {
		fprintf(stdout, "ERROR: large block read buffer cf_valloc()\n");
		return false;
	}

	return true;
}

//------------------------------------------------
// Create large block write buffers.
//
static bool create_salters() {
	if (! g_num_write_buffers) {
		if (! (g_salters[0].p_buffer = cf_valloc(g_large_block_ops_bytes))) {
			fprintf(stdout, "ERROR: large block write buffer cf_valloc()\n");
			return false;
		}

		memset(g_salters[0].p_buffer, 0, g_large_block_ops_bytes);
		g_num_write_buffers = 1;

		return true;
	}

	uint8_t seed_buffer[RAND_SEED_SIZE];

	if (! rand_seed(seed_buffer)) {
		return false;
	}
	uint32_t n;
	for (n = 0; n < g_num_write_buffers; n++) {
		if (! (g_salters[n].p_buffer = cf_valloc(g_large_block_ops_bytes))) {
			fprintf(stdout, "ERROR: large block write buffer cf_valloc()\n");
			return false;
		}

		if (! rand_fill(g_salters[n].p_buffer, g_large_block_ops_bytes)) {
			return false;
		}

		if (g_num_write_buffers > 1) {
			pthread_mutex_init(&g_salters[n].lock, NULL);
		}
	}

	return true;
}

//------------------------------------------------
// Destroy large block write buffers.
//
static void destroy_salters() {
	uint32_t n;
	for (n = 0; n < g_num_write_buffers; n++) {
		free(g_salters[n].p_buffer);

		if (g_num_write_buffers > 1) {
			pthread_mutex_destroy(&g_salters[n].lock);
		}
	}
}

//------------------------------------------------
// Discover device storage capacity.
//
static void discover_num_blocks(device* p_device) {
	int fd = p_device->fd;

	if (fd == -1) {
		p_device->num_512_blocks = 0;
		p_device->num_large_blocks = 0;
		return;
	}

	uint64_t device_bytes = 0;

	ioctl(fd, BLKGETSIZE64, &device_bytes);
	p_device->num_large_blocks = device_bytes / g_large_block_ops_bytes;
	p_device->num_512_blocks = 
		(p_device->num_large_blocks * g_large_block_ops_bytes) /
		MIN_BLOCK_BYTES;

	fprintf(stdout, "%s size = %" PRIu64 " bytes, %" PRIu64
			" 512-byte blocks, %" PRIu64 " large blocks\n", p_device->name,
			device_bytes, p_device->num_512_blocks, p_device->num_large_blocks);

}

//------------------------------------------------
// Get a random uint32_t.
//
static inline uint32_t rand_32() {
	return (uint32_t)rand_64();
}

//------------------------------------------------
// Get a random uint64_t.
//
static uint64_t rand_64() {
	return ((uint64_t)rand() << 16) | ((uint64_t)rand() & 0xffffULL);
}

//------------------------------------------------
// Fill a buffer (> 64 bytes) with random bits.
//
static bool rand_fill(uint8_t* p_buffer, uint32_t size) {
	if (RAND_bytes(p_buffer, size) != 1) {
		fprintf(stdout, "ERROR: RAND_bytes() failed\n");
		return false;
	}

	return true;
}

//------------------------------------------------
// Seed a buffer (> 64 bytes) for random fill.
//
static bool rand_seed(uint8_t* p_buffer) {
	int fd = open("/dev/urandom", O_RDONLY);

	if (fd == -1) {
		fprintf(stdout, "ERROR: can't open /dev/urandom\n");
		return false;
	}

	ssize_t read_result = read(fd, p_buffer, RAND_SEED_SIZE);

	if (read_result != (ssize_t)RAND_SEED_SIZE) {
		close(fd);
		fprintf(stdout, "ERROR: can't seed random number generator\n");
		return false;
	}

	close(fd);
	RAND_seed(p_buffer, read_result);

	return true;
}

//------------------------------------------------
// Get a random read offset for a device.
//
static uint64_t random_read_offset(device* p_device) {
	if (! p_device->num_512_blocks) {
		return 0;
	}

	uint64_t num_read_offsets =
		p_device->num_512_blocks - (uint64_t)g_read_req_num_512_blocks + 1;

	return (rand_64() % num_read_offsets) * MIN_BLOCK_BYTES;
}

//------------------------------------------------
// Get a random large block offset for a device.
//
static uint64_t random_large_block_offset(device* p_device) {
	if (! p_device->num_large_blocks) {
		return 0;
	}

	return (rand_64() % p_device->num_large_blocks) * g_large_block_ops_bytes;
}
//------------------------------------------------
//// Do one device read operation.
////
static uint64_t read_from_device(device* p_device, uint64_t offset,
		uint32_t size, uint8_t* p_buffer) {
	int fd = p_device->fd;

	if (fd == -1) {
		return -1;
	}

	if (lseek(fd, offset, SEEK_SET) != offset ||
			read(fd, p_buffer, size) != (ssize_t)size) {
		close(fd);
		fprintf(stdout, "ERROR: seek & read\n");
		return -1;
	}

	uint64_t stop_ms = cf_getms();

	return stop_ms;
}

//------------------------------------------------
// Do one large block read operation and report.
//
static void read_and_report_large_block(device* p_device) {
	uint64_t offset = random_large_block_offset(p_device);
	uint64_t start_time = cf_getms();
	uint64_t stop_time = read_from_device(p_device, offset,
			g_large_block_ops_bytes, p_device->p_large_block_read_buffer);

	if (stop_time != -1) {
		histogram_insert_data_point(g_p_large_block_read_histogram,
				safe_delta_ms(start_time, stop_time));
	}
}

//------------------------------------------------
// Check time differences.
//
static inline uint64_t safe_delta_ms(uint64_t start_ms, uint64_t stop_ms) {
	return start_ms > stop_ms ? 0 : stop_ms - start_ms;
}

//------------------------------------------------
// Set devices' system block schedulers.
//
static void set_schedulers() {
	const char* mode = SCHEDULER_MODES[g_scheduler_mode];
	size_t mode_length = strlen(mode);
	uint32_t d;
	for (d = 0; d < g_num_devices; d++) {
		const char* device_name = g_device_names[d];
		const char* p_slash = strrchr(device_name, '/');
		const char* device_tag = p_slash ? p_slash + 1 : device_name;

		char scheduler_file_name[128];

		strcpy(scheduler_file_name, "/sys/block/");
		strcat(scheduler_file_name, device_tag);
		strcat(scheduler_file_name, "/queue/scheduler");

		FILE* scheduler_file = fopen(scheduler_file_name, "w");

		if (! scheduler_file) {
			fprintf(stdout, "ERROR: couldn't open %s\n", scheduler_file_name);
			continue;
		}

		if (fwrite(mode, mode_length, 1, scheduler_file) != 1) {
			fprintf(stdout, "ERROR: writing %s to %s\n", mode,
					scheduler_file_name);
		}

		fclose(scheduler_file);
	}
}

//------------------------------------------------
// Do one large block write operation and report.
//
static void write_and_report_large_block(device* p_device) {
	salter* p_salter;

	if (g_num_write_buffers > 1) {
		p_salter = &g_salters[rand_32() % g_num_write_buffers];

		pthread_mutex_lock(&p_salter->lock);
		*(uint32_t*)p_salter->p_buffer = p_salter->stamp++;
	}
	else {
		p_salter = &g_salters[0];
	}

	uint64_t offset = random_large_block_offset(p_device);
	uint64_t start_time = cf_getms();
	uint64_t stop_time = write_to_device(p_device, offset,
			g_large_block_ops_bytes, p_salter->p_buffer);

	if (g_num_write_buffers > 1) {
		pthread_mutex_unlock(&p_salter->lock);
	}

	if (stop_time != -1) {
		histogram_insert_data_point(g_p_large_block_write_histogram,
				safe_delta_ms(start_time, stop_time));
	}
}

//------------------------------------------------
// Do one device write operation.
//
static uint64_t write_to_device(device* p_device, uint64_t offset,
		uint32_t size, uint8_t* p_buffer) {
	int fd = p_device->fd;

	if (fd == -1) {
		return -1;
	}

	if (lseek(fd, offset, SEEK_SET) != offset ||
			write(fd, p_buffer, size) != (ssize_t)size) {
		close(fd);
		fprintf(stdout, "ERROR: seek & write\n");
		return -1;
	}

	uint64_t stop_ms = cf_getms();

	//fd_put(p_device, fd);

	return stop_ms;
}


//==========================================================
// Debugging Helpers
//

static void as_sig_handle_segv(int sig_num) {
	fprintf(stdout, "Signal SEGV received: stack trace\n");

	void* bt[50];
	uint sz = backtrace(bt, 50);

	char** strings = backtrace_symbols(bt, sz);
	int i;
	for (i = 0; i < sz; ++i) {
		fprintf(stdout, "stacktrace: frame %d: %s\n", i, strings[i]);
	}

	free(strings);

	fflush(stdout);
	_exit(-1);
}

static void as_sig_handle_term(int sig_num) {
	fprintf(stdout, "Signal TERM received, aborting\n");

	void* bt[50];
	uint sz = backtrace(bt, 50);

	char** strings = backtrace_symbols(bt, sz);
	int i;
	for (i = 0; i < sz; ++i) {
		fprintf(stdout, "stacktrace: frame %d: %s\n", i, strings[i]);
	}

	free(strings);

	fflush(stdout);
	_exit(0);
}
