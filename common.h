#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#define _GNU_SOURCE
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>  
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>  
#include <sys/stat.h>
#include <sys/prctl.h>
#include <map>
#include <iostream>
#include <fstream>
using namespace std;

//#define __DEBUG
//#define __OUT
//#define __MUTEX
//#define _TEST_SYN
#define __TIMEOUT_SEND
//#define __BITMAP
//#define __STRONG_FLOW_CONTROL
#define __polling
//#define __resub

#ifdef __DEBUG
#define DEBUG(info,...)    printf(info, ##__VA_ARGS__)
#else
#define DEBUG(info,...)
#endif

#ifdef __OUT
#define OUT(info,...)    printf(info, ##__VA_ARGS__)
#else
#define OUT(info,...)
#endif

#ifdef __resub
#define resub(info,...)    printf(info, ##__VA_ARGS__)
#else
#define resub(info,...)
#endif

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define TIMEOUT_IN_MS 500
typedef unsigned int uint;
typedef unsigned long long ull;
typedef unsigned char uchar;

struct ScatterList
{
	struct ScatterList *next;  //链表的形式实现
	void *address;             //数据存放区域的页内地址
	int length;                //数据存放区域的长度
};

struct connection
{
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_cq **cq_data, **cq_ctrl, **cq_mem;
	struct ibv_comp_channel *comp_channel, *mem_channel;
	struct ibv_port_attr		port_attr;	
};

struct memory_management
{
	int number;
	int memory_reback_flag;
	
	struct ScatterList application;
	
	struct ibv_mr *rdma_send_mr;
	struct ibv_mr *rdma_recv_mr;
	
	struct ibv_mr *send_mr;
	struct ibv_mr *recv_mr;
	
	struct ibv_mr peer_mr;
	
	char *recv_buffer;
	char *send_buffer;
	
	char *rdma_send_region;
	char *rdma_recv_region;	
	
	struct bitmap *send, *peer[10], *peer_using[10], *peer_used[10];
	void **mapping_table[10];
	
	pthread_mutex_t mapping_mutex[10], flag_mutex;
};

struct qp_management
{
	int data_num;
	int ctrl_num;
	struct ibv_qp *qp[128];
};

// request <=> task < scatter < package


extern struct connection *s_ctx;
extern struct memory_management *memgt;
extern struct qp_management *qpmgt;
extern struct rdma_cm_event *event;
extern struct rdma_event_channel *ec;
extern struct rdma_cm_id *conn_id[128], *listener[128];
extern int end;//active 0 backup 1
/* both */
extern int bind_port;
extern int BUFFER_SIZE;
extern int TOTAL_SIZE;
extern int BUFFER_SIZE_EXTEND;
extern int RDMA_BUFFER_SIZE;
extern int thread_number;
extern int connect_number;
extern int buffer_per_size;
extern int ctrl_number;
extern int full_time_interval;
extern int recv_buffer_num;//主从两端每个qp控制数据缓冲区个数
extern int package_pool_size;
extern int cq_ctrl_num;
extern int cq_data_num;
extern int cq_size;
extern int qp_size;
extern int qp_size_limit;
extern int concurrency_num;
extern int memory_reback_number;
extern ull magic_number;
/* active */
extern int resend_limit;
extern int request_size;
extern int scatter_size;
extern int package_size;
extern int recv_imm_data_num;//主端接收从端imm_data wr个数
extern int request_buffer_size;
extern int scatter_buffer_size;
extern int task_pool_size;
extern int scatter_pool_size;
/* backup */
extern int ScatterList_pool_size;
extern int request_pool_size;

int on_connect_request(struct rdma_cm_id *id, int tid);
int on_connection(struct rdma_cm_id *id, int tid);
int on_addr_resolved(struct rdma_cm_id *id, int tid);
int on_route_resolved(struct rdma_cm_id *id, int tid);
void build_connection(struct rdma_cm_id *id, int tid);
void build_context(struct ibv_context *verbs);
void build_params(struct rdma_conn_param *params);
void register_memory( int tid );
void post_recv( int qp_id, ull tid, int offset, int recv_size);
void post_send( int qp_id, ull tid, int offset, int send_size, int imm_data );
void post_rdma_write( int qp_id, struct scatter_active *sct );
void send_package( struct package_active *now, int ps, int offset, int qp_id  );
void die(const char *reason);
int get_wc( struct ibv_wc *wc );
int qp_query( int qp_id );
int re_qp_query( int qp_id );
int query_bit_free( uint *bit, int offset, int size );
int update_bit( uint *bit, int offset, int size, int *data, int len );
int destroy_qp_management();
int destroy_connection();
int destroy_memory_management(int end);
double elapse_sec();
uchar lowbit( uchar x );
int init_bitmap( struct bitmap **btmp, int size );
int final_bitmap( struct bitmap *btmp );
int query_bitmap( struct bitmap *btmp );
int update_bitmap( struct bitmap *btmp, int *data, int len );
int query_bitmap_free( struct bitmap *btmp );


int max( int a, int b );
int query_qp_count( struct qp_management *mgt, int id );
void inc_qp_count( struct qp_management *mgt, int id );
void dec_qp_count( struct qp_management *mgt, int id );

void get_arg();
int get_wc_b( struct ibv_wc *wc );
void cond_wait( pthread_mutex_t* mutex, pthread_cond_t* cond );
#endif
