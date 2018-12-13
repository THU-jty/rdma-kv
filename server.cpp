#include "common.h"


struct sockaddr_in6 addr;
pthread_t completion_id, mem_ctrl_id;
int nofity_number, shut_down;

map<int,string>msp;

extern int recv_package, send_package_ack;
extern double send_time, recv_time, cmt_time;
extern struct target_rate polling;

void initialize_backup( void (*f)(struct request_backup *request), void *address, int length );
int on_event(struct rdma_cm_event *event, int tid);
void *completion_backup(void *);
void (*commit)( struct request_backup *request );
void notify( struct request_backup *request );
void *memory_ctrl_backup();

int on_event(struct rdma_cm_event *event, int tid)
{
	int r = 0;
	if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
	  r = on_connect_request(event->id, tid);
	else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
	  r = on_connection(event->id, tid);
	// else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
	  // r = on_disconnect(event->id);
	else
	  die("on_event: unknown event.");

	return r;
}

void initialize_backup( void (*f)(struct request_backup *request), void *address, int length )
{
	end = 1;
	nofity_number = 0;
	int port = 0;
	memset(&addr, 0, sizeof(addr));
	addr.sin6_family = AF_INET6;
	addr.sin6_port  = htons(bind_port);
	memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	memgt->application.next = NULL;
	memgt->application.address = address;
	memgt->application.length = length;
	
	struct ibv_wc wc;
	int i = 0, j;
	for( i = 0; i < connect_number; i ++ ){
		TEST_Z(ec = rdma_create_event_channel());
		TEST_NZ(rdma_create_id(ec, &listener[i], NULL, RDMA_PS_TCP));
		TEST_NZ(rdma_bind_addr(listener[i], (struct sockaddr *)&addr));
		TEST_NZ(rdma_listen(listener[i], 10)); /* backlog=10 is arbitrary */
		port = ntohs(rdma_get_src_port(listener[i]));
		fprintf(stderr, "port#%d: %d\n", i, port);
		if( i == 0 ){
			printf("listening on port %d.\n", port);
			memset(&addr, 0, sizeof(addr));
			addr.sin6_family = AF_INET6;
		}
		else{
			memcpy( memgt->send_buffer, &port, sizeof(int) );
			//fprintf(stderr, "port#%d: %d\n", i, *((int *)memgt->send_buffer));
			if(1){
				post_send( 0, port, 0, sizeof(int), 0 );
				int tmp = get_wc( &wc );
			}
			printf("post send ok\n");
		}
		
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
	memcpy( memgt->send_buffer, memgt->rdma_recv_mr, sizeof(struct ibv_mr) );
	if(1){
		post_send( 0, 50, 0, sizeof(struct ibv_mr), 0 );
		int tmp = get_wc( &wc );
	}
	
	printf("add: %p length: %d\n", memgt->rdma_recv_mr->addr,
	memgt->rdma_recv_mr->length);
	
	pthread_create( &completion_id, NULL, completion_backup, NULL );
}

void finalize_backup()
{
	fprintf(stderr, "finalize begin\n");
	shut_down = 1;
	pthread_join( completion_id, NULL );
	
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

void *completion_backup(void *)
{
	prctl(PR_SET_NAME, "completion_backup");
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, r_pos, p_pos, SL_pos, cnt = 0, data[128], tot = 0, send_count = 0;
	post_recv( 0, 0, 0, 0 );	
	fstream in;
	in.open( "data", ios::in );
	if( in.is_open() ){
		while( !in.eof() ){
			int x; string s;
			in >> x >> s;
			if( in.fail() ) break;
			msp[x] = s;
			cout << x << "  " << msp[x] << endl;
		}
	}
	printf("completion thread ready\n");
	while(!shut_down){
		cq = s_ctx->cq_data[0];
		do{
			int num = ibv_poll_cq(cq, 100, wc_array);			
			if( num <= 0 ) break;
			//fprintf(stderr, "%04d CQE get!!!\n", num);
			for( k = 0; k < num; k ++ ){
				wc = &wc_array[k];
				// switch (wc->opcode) {
					// case IBV_WC_RECV_RDMA_WITH_IMM: fprintf(stderr, "IBV_WC_RECV_RDMA_WITH_IMM\n"); break;
					// case IBV_WC_RDMA_WRITE: fprintf(stderr, "IBV_WC_RDMA_WRITE\n"); break;
					// case IBV_WC_RDMA_READ: fprintf(stderr, "IBV_WC_RDMA_READ\n"); break;
					// case IBV_WC_SEND: fprintf(stderr, "IBV_WC_SEND\n"); break;
					// case IBV_WC_RECV: fprintf(stderr, "IBV_WC_RECV\n"); break;
					// default : fprintf(stderr, "unknwon\n"); break;
				// }
				if( wc->opcode == IBV_WC_SEND ){
					if( wc->status != IBV_WC_SUCCESS ){
						printf("failure send\n");
						continue;
					}
				}
				
				if( wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM  ){
					if( wc->status != IBV_WC_SUCCESS ){
						printf("recv send\n");
						continue;
					}
					string s;
					char *p = memgt->rdma_recv_region;
					if( wc->imm_data == 1 ){//put
						int k = (*(int*)p); p += sizeof(int);
						s.clear();
						int len = (*(int*)p); p += sizeof(int);
						for( i = 0; i < len; i ++ ){
							s.push_back( p[i] );
						}
						msp[ k ] = s;
						post_send( 0, 0, 0, 0, 0 );
						post_recv( 0, 0, 0, 0 );
						cout << "put" << " " << k << " " << s << endl;
					}
					else if( wc->imm_data == 2 ){//get
						int k = (*(int*)p); p += sizeof(int);
						map<int,string>::iterator it;
						it = msp.find(k);
						if( it == msp.end() ) s = "NULL";
						else s = msp[k];
						int len = s.length();
						char *q = memgt->send_buffer;
						*( (int*)q ) = len; q += sizeof(int);
						s.copy( q, len, 0 );
						post_send( 0, 0, 0, len+sizeof(int), 0 );
						post_recv( 0, 0, 0, 0 );
						cout << "get" << " " << k << " " << s << endl;
					}
					else if( wc->imm_data == 3 ){//del
						int k = (*(int*)p); p += sizeof(int);
						map<int,string>::iterator it;
						it = msp.find(k);
						if( it != msp.end() ){
							msp.erase(it);
						}
						post_send( 0, 0, 0, 0, 0 );
						post_recv( 0, 0, 0, 0 );
						cout << "delete" << " " << k << endl;
					}
					else{//query
						int st = (*(int*)p); p += sizeof(int);
						int ed = (*(int*)p); p += sizeof(int);
						map<int,string>::iterator it;
						it = msp.lower_bound(st);
						int cnt = 0;
						char *q = memgt->send_buffer; q += sizeof(int);
						it = msp.lower_bound(st);
						if( it != msp.end() )
							for( it = msp.lower_bound(st); it != msp.end() && it->first >= st && it->first < ed; it ++ ){
								*( (int*)q ) = it->first; q += sizeof(int);
								s = it->second;
								*( (int*)q ) = s.length(); q += sizeof(int);
								s.copy( q, s.length(), 0 ); q += s.length();
								cnt ++;
								cout << "query" << " " << it->first << " " << s << endl;
							}
						*((int*)memgt->send_buffer) = cnt;
						post_send( 0, 0, 0, q-memgt->send_buffer, 0 );
						post_recv( 0, 0, 0, 0 );
					}
				}
			}
		}while(1);	
	}
	fstream out;
	out.open( "data", ios::trunc|ios::out );
	map<int,string>::iterator it;
	for( it = msp.begin(); it != msp.end(); it ++ ){
		if( it->second.length() > 0 ){
			out << it->first << "\t" << it->second << endl;
			cout << it->first << "\t" << it->second << endl;
		}
	}
	out.close();
	printf("data into disk\n");
}

int main()
{
	struct ScatterList SL;
	SL.address = ( void * )malloc( RDMA_BUFFER_SIZE );
	SL.length = RDMA_BUFFER_SIZE;
	initialize_backup( NULL, SL.address, SL.length );
	string s;
	//while(1);
	cin >> s;
	finalize_backup();
}
