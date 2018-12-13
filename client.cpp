#include "common.h"

struct thread_pool
{
	int number, tmp[10], shutdown;
	pthread_cond_t cond0[10], cond1[10];
	pthread_t completion_id, pthread_id[10], mem_ctrl_id;
	pthread_mutex_t qp_count_mutex[10];
	pthread_cond_t qp_cond0[10], qp_cond1[10];
};

struct rdma_addrinfo *addr;
struct thread_pool *thpl;
int send_package_count, write_count, request_count, send_token;
struct timeval test_start;
extern double get_working, do_working, \
cq_send, cq_recv, cq_write, cq_waiting, cq_poll, q_task, other,\
send_package_time, end_time, base, working_write, q_qp,\
init_remote, init_scatter, q_scatter, one_rq_end, one_rq_start,\
sum_tran, sbf_time, callback_time, get_request;
extern int d_count, send_new_id, rq_sub, mx, q_count;
extern struct target_rate polling_active;

void initialize_active( void *address, int length, char *ip_address );
int on_event(struct rdma_cm_event *event, int tid);
void *working_thread(void *arg);
void *completion_active();
void huawei_send( struct request_active *rq );
void *full_time_send();
int clean_send_buffer( struct package_active *now );
int clean_package( struct package_active *now );
void *memory_ctrl_active();

int on_event(struct rdma_cm_event *event, int tid)
{
	int r = 0;
    //printf("on event %d\n", event->event);
	if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
        r = on_addr_resolved(event->id, tid);
    else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
      r = on_route_resolved(event->id, tid);
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
      r = on_connection(event->id, tid);
	// else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
	  // r = on_disconnect(event->id);
	else
	  die("on_event: unknown event.");

	return r;
}

void initialize_active( void *address, int length, char *ip_address )
{
	end = 0;
	memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	memgt->application.next = NULL;
	memgt->application.address = address;
	memgt->application.length = length;
	
	/*建立连接，数量为connect_number，\
	每次通过第0条链路传输port及ibv_mr，第0条链路端口固定为bind_port*/
	struct ibv_wc wc;
	for( int i = 0; i < connect_number; i ++ ){
		if( i == 0 ){
			char port[20];
			sprintf(port, "%d\0", bind_port);
			TEST_NZ(rdma_getaddrinfo(ip_address, port, NULL, &addr));
		}
		else{
			if(1){
				post_recv( 0, i, 0, sizeof(int));
				int tmp = get_wc( &wc );
			}
			
			char port[20];
			fprintf(stderr, "port: %d\n", *((int *)(memgt->recv_buffer)));
			int tmp = *((int *)(memgt->recv_buffer));
			sprintf(port, "%d\0", tmp);
			TEST_NZ(rdma_getaddrinfo(ip_address, port, NULL, &addr));
		}
		TEST_Z(ec = rdma_create_event_channel());
		TEST_NZ(rdma_create_id(ec, &conn_id[i], NULL, RDMA_PS_TCP));
		TEST_NZ(rdma_resolve_addr(conn_id[i], NULL, addr->ai_dst_addr, TIMEOUT_IN_MS));
		rdma_freeaddrinfo(addr);
		while (rdma_get_cm_event(ec, &event) == 0) {
			struct rdma_cm_event event_copy;
			memcpy(&event_copy, event, sizeof(*event));
			rdma_ack_cm_event(event);
			if (on_event(&event_copy, i)){
				break;
			}
		}
		fprintf(stderr, "build connect succeed %d\n", i);
	}
	
	if(1){
		post_recv( 0, 20, 0, sizeof(struct ibv_mr));
		int tmp = get_wc( &wc );
	}
	
	memcpy( &memgt->peer_mr, memgt->recv_buffer, sizeof(struct ibv_mr) );
	printf("peer add: %p length: %d\n", memgt->peer_mr.addr,
	memgt->peer_mr.length);

	fprintf(stderr, "create pthread pool end\n");
}

void finalize_active()
{
	/* destroy qp management */
	destroy_qp_management();
	fprintf(stderr, "destroy qp management success\n");
	
	/* destroy memory management */
	destroy_memory_management(end);
	fprintf(stderr, "destroy memory management success\n");
		
	/* destroy connection struct */
	destroy_connection();
	fprintf(stderr, "destroy connection success\n");
	fprintf(stderr, "finalize end\n");
}

void put()
{
	post_recv( 0, 0, 0, 1024*1024 );
	
	int id, len;
	int i, j;
	string s;
	cin >> id >> s;
	len = s.length();
	char *p = memgt->rdma_send_region;
	memcpy( p, &id, sizeof(int) ); p += sizeof(int);
	memcpy( p, &len, sizeof(int) ); p += sizeof(int);
	s.copy( p, len, 0 );
	
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge[10];
	
	memset(&wr, 0, sizeof(wr));
	
	wr.wr_id = 0;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = (uintptr_t)memgt->peer_mr.addr;
	wr.wr.rdma.rkey = memgt->peer_mr.rkey;
	
	wr.imm_data = 1;
	wr.sg_list = sge;
	wr.num_sge = 1;
	
	sge[0].addr = (uintptr_t)memgt->rdma_send_region;
	sge[0].length = len+2*sizeof(int);
	sge[0].lkey = memgt->rdma_send_mr->lkey;

redo:	
	TEST_NZ(ibv_post_send(qpmgt->qp[0], &wr, &bad_wr));
	
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	int fl = 0;
	cq = s_ctx->cq_data[0];
	while(!fl){
		int num = ibv_poll_cq(cq, 100, wc_array);
		if( num < 0 ) continue;
		for( int k = 0; k < num; k ++ ){
			wc = &wc_array[k];
			if( wc->opcode == IBV_WC_RDMA_WRITE ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("write error redo now!\n");
					goto redo;
				}
				continue;
			}
			if( wc->opcode == IBV_WC_RECV ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("recv error redo now!\n");
					exit(1);
				}
				fl = 1;
			}
		}
	}
	cout << "put ok" << endl;
}

void get()
{
	post_recv( 0, 0, 0, 1024*1024 );
	
	int id, len;
	int i, j;
	string s;
	cin >> id;
	char *p = memgt->rdma_send_region;
	memcpy( p, &id, sizeof(int) ); p += sizeof(int);
	
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge[10];
	
	memset(&wr, 0, sizeof(wr));
	
	wr.wr_id = 0;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = (uintptr_t)memgt->peer_mr.addr;
	wr.wr.rdma.rkey = memgt->peer_mr.rkey;
	
	wr.imm_data = 2;
	wr.sg_list = sge;
	wr.num_sge = 1;
	
	sge[0].addr = (uintptr_t)memgt->rdma_send_region;
	sge[0].length = sizeof(int);
	sge[0].lkey = memgt->rdma_send_mr->lkey;

redo:	
	TEST_NZ(ibv_post_send(qpmgt->qp[0], &wr, &bad_wr));
	
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	int fl = 0;
	cq = s_ctx->cq_data[0];
	while(!fl){
		int num = ibv_poll_cq(cq, 100, wc_array);
		if( num < 0 ) continue;
		for( int k = 0; k < num; k ++ ){
			wc = &wc_array[k];
			if( wc->opcode == IBV_WC_RDMA_WRITE ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("write error redo now!\n");
					goto redo;
				}
				continue;
			}
			if( wc->opcode == IBV_WC_RECV ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("recv error redo now!\n");
					exit(1);
				}
				fl = 1;
			}
		}
	}
	cout << "get ok" << endl;
	p = memgt->recv_buffer;
	memcpy( &len, p, sizeof(int) ); p += sizeof(int);
	s.clear();
	for( i = 0; i < len; i ++ ){
		s.push_back( p[i] );
	}
	cout << "value: " << s << endl;
}

void del()
{
	post_recv( 0, 0, 0, 1024*1024 );
	
	int id, len;
	int i, j;
	string s;
	cin >> id;
	char *p = memgt->rdma_send_region;
	memcpy( p, &id, sizeof(int) ); p += sizeof(int);
	
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge[10];
	
	memset(&wr, 0, sizeof(wr));
	
	wr.wr_id = 0;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = (uintptr_t)memgt->peer_mr.addr;
	wr.wr.rdma.rkey = memgt->peer_mr.rkey;
	
	wr.imm_data = 3;
	wr.sg_list = sge;
	wr.num_sge = 1;
	
	sge[0].addr = (uintptr_t)memgt->rdma_send_region;
	sge[0].length = sizeof(int);
	sge[0].lkey = memgt->rdma_send_mr->lkey;

redo:	
	TEST_NZ(ibv_post_send(qpmgt->qp[0], &wr, &bad_wr));
	
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	int fl = 0;
	cq = s_ctx->cq_data[0];
	while(!fl){
		int num = ibv_poll_cq(cq, 100, wc_array);
		if( num < 0 ) continue;
		for( int k = 0; k < num; k ++ ){
			wc = &wc_array[k];
			if( wc->opcode == IBV_WC_RDMA_WRITE ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("write error redo now!\n");
					goto redo;
				}
				continue;
			}
			if( wc->opcode == IBV_WC_RECV ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("recv error redo now!\n");
					exit(1);
				}
				fl = 1;
			}
		}
	}
	cout << "delete ok" << endl;
}

void qry()
{
	post_recv( 0, 0, 0, 1024*1024 );
	
	int st, ed, len, i, j;
	string s;
	cin >> st >> ed;
	if( st > ed ) swap( st, ed );
	char *p = memgt->rdma_send_region;
	memcpy( p, &st, sizeof(int) ); p += sizeof(int);
	memcpy( p, &ed, sizeof(int) ); p += sizeof(int);
	
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge[10];
	
	memset(&wr, 0, sizeof(wr));
	
	wr.wr_id = 0;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = (uintptr_t)memgt->peer_mr.addr;
	wr.wr.rdma.rkey = memgt->peer_mr.rkey;
	
	wr.imm_data = 4;
	wr.sg_list = sge;
	wr.num_sge = 1;
	
	sge[0].addr = (uintptr_t)memgt->rdma_send_region;
	sge[0].length = sizeof(int)*2;
	sge[0].lkey = memgt->rdma_send_mr->lkey;

redo:	
	TEST_NZ(ibv_post_send(qpmgt->qp[0], &wr, &bad_wr));
	
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	int fl = 0;
	cq = s_ctx->cq_data[0];
	while(!fl){
		int num = ibv_poll_cq(cq, 100, wc_array);
		if( num < 0 ) continue;
		for( int k = 0; k < num; k ++ ){
			wc = &wc_array[k];
			if( wc->opcode == IBV_WC_RDMA_WRITE ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("write error redo now!\n");
					goto redo;
				}
				continue;
			}
			if( wc->opcode == IBV_WC_RECV ){
				if( wc->status != IBV_WC_SUCCESS ){
					printf("recv error redo now!\n");
					exit(1);
				}
				fl = 1;
			}
		}
	}
	cout << "query ok" << endl;
	p = memgt->recv_buffer;
	memcpy( &len, p, sizeof(int) ); p += sizeof(int);
	for( i = 0; i < len; i ++ ){
		int x = (*(int*)p); p += sizeof(int);
		int y = (*(int*)p); p += sizeof(int);
		s.clear();
		for( j = 0; j < y; j ++ ){
			s.push_back( p[j] );
		}
		cout << "key: " << x << " value: " << s << endl;
	}
}

int main( int argc, char **argv )
{
	struct ScatterList SL;
	if( argc != 2 ){
		printf("error input\n");
		exit(1);
	}
	SL.address = ( void * )malloc( RDMA_BUFFER_SIZE );
	SL.length = RDMA_BUFFER_SIZE;
	initialize_active( SL.address, SL.length, argv[1] );
	while(1){
		string s;
		cin >> s;
		if( s == "exit" ) break;
		else if( s == "put" ) put();
		else if( s == "get" ) get();
		else if( s == "del" ) del();
		else if( s == "qry" ) qry();
		else puts("wrong input\n");
	}
	finalize_active();
}