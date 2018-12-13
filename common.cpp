#include "common.h"


struct connection *s_ctx;
struct memory_management *memgt;
struct qp_management *qpmgt;
struct rdma_cm_event *event;
struct rdma_event_channel *ec;
struct rdma_cm_id *conn_id[128], *listener[128];
int end;//active 0 backup 1

int bind_port = 45679;
int BUFFER_SIZE = 2*1024*1024;
int BUFFER_SIZE_EXTEND = 1*1024*1024;
int RDMA_BUFFER_SIZE = 1024*1024*64;
int thread_number = 1;
int connect_number = 1;
int ctrl_number = 0;
int cq_ctrl_num = 0;
int cq_data_num = 1;
int cq_size = 500;
int qp_size = 500;
int qp_size_limit = 1;
int concurrency_num = 5;
int memory_reback_number = 20;
ull magic_number = 0x0145145145145145;
int ib_port = 1;

int buffer_per_size;

int recv_buffer_num = 200;
int recv_imm_data_num = 50;
int package_pool_size = 8000;
int full_time_interval = 1000;//us 满时重传时间间隔

int resend_limit = 3;
int request_size = 256*1024;//B
int scatter_size = 4;
int package_size = 4; 
int request_buffer_size = 300000;
int scatter_buffer_size = 1024;
int task_pool_size = 16000;
int scatter_pool_size = 8000;

int ScatterList_pool_size = 16000;
int request_pool_size = 16000;

extern double query;
/*
BUFFER_SIZE >= recv_buffer_num*buffer_per_size*ctrl_number
task: 8192/thread_number
scatter: 8192/scatter_size/thread_number
remote area: RDMA_BUFFER_SIZE/request_size/scatter_size/thread_number
package: 8192
send buffer: BUFFER_SIZE/buffer_per_size
*/

int sock;

static int resources_create(char *ip_address, )
{	
	struct ibv_device       **dev_list = NULL;
	struct ibv_qp_init_attr qp_init_attr;
	struct ibv_device 	*ib_dev = NULL;
	size_t 			size;
	int 			i;
	int 			mr_flags = 0;
	int 			cq_size = 0;
	int 			num_devices;
	char *dev_name = "mlx5_1";
	
	/* if client side */
	if ( end == 0 ) {
		sock = sock_client_connect(ip_address, bind_port);
		if (sock < 0) {
			fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n", 
				ip_address bind_port);
			return -1;
		}
	} else {
		fprintf(stdout, "waiting on port %d for TCP connection\n", bind_port);

		sock = sock_daemon_connect(bind_port);
		if (sock < 0) {
			fprintf(stderr, "failed to establish TCP connection with client on port %d\n", 
				bind_port);
			return -1;
		}
	}

	fprintf(stdout, "TCP connection was established\n");

	fprintf(stdout, "searching for IB devices in host\n");

	/* get device names in the system */
	dev_list = ibv_get_device_list(&num_devices);
	if (!dev_list) {
		fprintf(stderr, "failed to get IB devices list\n");
		return 1;
	}

	/* if there isn't any IB device in host */
	if (!num_devices) {
		fprintf(stderr, "found %d device(s)\n", num_devices);
		return 1;
	}

	fprintf(stdout, "found %d device(s)\n", num_devices);

	/* search for the specific device we want to work with */
	for (i = 0; i < num_devices; i ++) {
		if (!dev_name) {
			dev_name = strdup(ibv_get_device_name(dev_list[i])); 
			fprintf(stdout, "device not specified, using first one found: %s\n", dev_name);
		}
		if (!strcmp(ibv_get_device_name(dev_list[i]), dev_name)) {
			ib_dev = dev_list[i];
			break;
		}
	}

	/* if the device wasn't found in host */
	if (!ib_dev) {
		fprintf(stderr, "IB device %s wasn't found\n", dev_name);
		return 1;
	}

	/* get device handle */
	s_ctx->ctx = ibv_open_device(ib_dev);
	if (!s_ctx->ctx) {
		fprintf(stderr, "failed to open device %s\n", dev_name);
		return 1;
	}

	/* We are now done with device list, free it */
	ibv_free_device_list(dev_list);
	dev_list = NULL;
	ib_dev = NULL;

	/* query port properties  */
	if (ibv_query_port(s_ctx->ctx, ib_port, &s_ctx->port_attr)) {
		fprintf(stderr, "ibv_query_port on port %u failed\n", ib_port);
		return 1;
	}

	/* allocate Protection Domain */
	s_ctx->pd = ibv_alloc_pd(s_ctx->ctx);
	if (!s_ctx->pd ) {
		fprintf(stderr, "ibv_alloc_pd failed\n");
		return 1;
	}

	

	return 0;
}




int on_connect_request(struct rdma_cm_id *id, int tid)
{
	struct rdma_conn_param cm_params;
	if(!tid) printf("received connection request.\n");
	build_connection(id, tid);
	conn_id[tid] = id;
	build_params(&cm_params);
	TEST_NZ(rdma_accept(id, &cm_params));
	return 0;
}

int on_addr_resolved(struct rdma_cm_id *rid, int tid)
{
	if(!tid) printf("address resolved.\n");
	build_connection(rid, tid);
	conn_id[tid] = rid;
	TEST_NZ(rdma_resolve_route(rid, TIMEOUT_IN_MS));
	return 0;
}

int on_route_resolved(struct rdma_cm_id *id, int tid)
{
	struct rdma_conn_param cm_params;
	if(!tid) printf("route resolved.\n");
	build_params(&cm_params);
	TEST_NZ(rdma_connect(id, &cm_params));
	
	if(!tid) printf("route resolved ok.\n");
	return 0;
}

int on_connection(struct rdma_cm_id *id, int tid)
{	
	return 1;
}

void build_connection(struct rdma_cm_id *id, int tid)
{
	struct ibv_qp_init_attr *qp_attr;
	qp_attr = ( struct ibv_qp_init_attr* )malloc( sizeof( struct ibv_qp_init_attr ) );
	if( !tid ){
	  build_context(id->verbs);
	  qpmgt->data_num = connect_number-ctrl_number;
	  qpmgt->ctrl_num = ctrl_number;
	  //sth need to init for 1st time
	}
	memset(qp_attr, 0, sizeof(*qp_attr));
	
	qp_attr->qp_type = IBV_QPT_RC;
	
	if( tid < qpmgt->data_num ){
		qp_attr->send_cq = s_ctx->cq_data[tid%cq_data_num];
		qp_attr->recv_cq = s_ctx->cq_data[tid%cq_data_num];
	}
	else if( tid < qpmgt->data_num+qpmgt->ctrl_num ){
		qp_attr->send_cq = s_ctx->cq_ctrl[tid%cq_ctrl_num];
		qp_attr->recv_cq = s_ctx->cq_ctrl[tid%cq_ctrl_num];
	}
	else{
		qp_attr->send_cq = s_ctx->cq_mem[0];
		qp_attr->recv_cq = s_ctx->cq_mem[0];
	}

	qp_attr->cap.max_send_wr = qp_size;
	qp_attr->cap.max_recv_wr = qp_size;
	qp_attr->cap.max_send_sge = 20;
	qp_attr->cap.max_recv_sge = 20;
	qp_attr->cap.max_inline_data = 200;
	
	qp_attr->sq_sig_all = 1;
	
	TEST_NZ(rdma_create_qp(id, s_ctx->pd, qp_attr));
	qpmgt->qp[tid] = id->qp;
	
	// struct ibv_qp_attr tmp[1];
	// enum ibv_qp_attr_mask mask;
	// struct ibv_qp_init_attr init[1];
	// mask = ( IBV_QP_RNR_RETRY | IBV_QP_RETRY_CNT | IBV_QP_QKEY | IBV_QP_PORT | IBV_QP_DEST_QPN );
	// ibv_query_qp( qpmgt->qp[tid], tmp, mask, init );
	// //printf("%d init rnr %d retry %d\n", tid, init->rnr_retry, init->retry_cnt);
	// printf("%d tmp rnr %d retry %d %d %d %d\n", tid, (int)tmp->rnr_retry, (int)tmp->retry_cnt, (int)tmp->dest_qp_num, (int)tmp->qkey, (int)tmp->port_num);
	// //cout << tmp->rnr_retry << "  " << tmp->retry_cnt << endl;
	
	if( !tid )
		register_memory( end );
}

void build_context(struct ibv_context *verbs)
{
	if (s_ctx) {
	  if (s_ctx->ctx != verbs)
		die("cannot handle events in more than one context.");
	  return;
	}
	s_ctx = ( struct connection * )malloc( sizeof( struct connection ) );
	
	s_ctx->ctx = verbs;

	TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
	TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
	TEST_Z(s_ctx->mem_channel = ibv_create_comp_channel(s_ctx->ctx));
	/* pay attention to size of CQ */
	s_ctx->cq_data = (struct ibv_cq **)malloc(sizeof(struct ibv_cq *)*cq_data_num);
	s_ctx->cq_ctrl = (struct ibv_cq **)malloc(sizeof(struct ibv_cq *)*cq_ctrl_num);
	s_ctx->cq_mem = (struct ibv_cq **)malloc(sizeof(struct ibv_cq *)*2);
	
	
	for( int i = 0; i < cq_data_num; i ++ ){
		TEST_Z(s_ctx->cq_data[i] = ibv_create_cq(s_ctx->ctx, cq_size, NULL, s_ctx->comp_channel, 0)); 
#ifndef __polling			
		TEST_NZ(ibv_req_notify_cq(s_ctx->cq_data[i], 0));
#endif		
	}
	for( int i = 0; i < cq_ctrl_num; i ++ ){
		TEST_Z(s_ctx->cq_ctrl[i] = ibv_create_cq(s_ctx->ctx, cq_size, NULL, s_ctx->comp_channel, 0)); 
#ifndef __polling			
		TEST_NZ(ibv_req_notify_cq(s_ctx->cq_ctrl[i], 0));
#endif			
	}
	TEST_Z(s_ctx->cq_mem[0] = ibv_create_cq(s_ctx->ctx, cq_size, NULL, s_ctx->mem_channel, 0)); 
	TEST_NZ(ibv_req_notify_cq(s_ctx->cq_mem[0], 0));
}

void build_params(struct rdma_conn_param *params)
{
	memset(params, 0, sizeof(*params));

	params->initiator_depth = params->responder_resources = 1;
	params->rnr_retry_count = 7; /* infinite retry */
	params->retry_count = 7; /* infinite retry */
}

void register_memory( int tid )// 0 active 1 backup
{
	memgt->recv_buffer = (char *)malloc(BUFFER_SIZE+BUFFER_SIZE_EXTEND);
	TEST_Z( memgt->recv_mr = ibv_reg_mr( s_ctx->pd, memgt->recv_buffer,
	BUFFER_SIZE+BUFFER_SIZE_EXTEND, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE ) );
	
	memgt->send_buffer = (char *)malloc(BUFFER_SIZE+BUFFER_SIZE_EXTEND);
	TEST_Z( memgt->send_mr = ibv_reg_mr( s_ctx->pd, memgt->send_buffer,
	BUFFER_SIZE+BUFFER_SIZE_EXTEND, IBV_ACCESS_LOCAL_WRITE ) );
	
	buffer_per_size = 4+4+(sizeof(void *)+sizeof(struct ScatterList))*scatter_size*package_size;
	//buffer_per_size = request_size;
	
	if( tid == 1 ){//active don't need recv
		TEST_Z( memgt->rdma_recv_mr = ibv_reg_mr( s_ctx->pd, memgt->application.address,
		memgt->application.length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE ) );
		memgt->rdma_recv_region = (char*)memgt->application.address;
	}
	else{
		TEST_Z( memgt->rdma_send_mr = ibv_reg_mr( s_ctx->pd, memgt->application.address,
		memgt->application.length, IBV_ACCESS_LOCAL_WRITE ) );
		memgt->rdma_send_region = (char*)memgt->application.address;
	}
}

void post_recv( int qp_id, ull tid, int offset, int recv_size)
{
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	
	wr.wr_id = tid;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	
	sge.addr = (uintptr_t)memgt->recv_buffer+offset;
	sge.length = recv_size;
	sge.lkey = memgt->recv_mr->lkey;
	
	TEST_NZ(ibv_post_recv(qpmgt->qp[qp_id], &wr, &bad_wr));
}

void post_send( int qp_id, ull tid, int offset, int send_size, int imm_data )
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));
	
	wr.wr_id = tid;
	wr.opcode = IBV_WR_SEND_WITH_IMM;
	wr.sg_list = &sge;
	wr.send_flags = IBV_SEND_SIGNALED;
	if( imm_data != 0 )
		wr.imm_data = imm_data;
	wr.num_sge = 1;
	
	sge.addr = (uintptr_t)memgt->send_buffer+offset;
	sge.length = send_size;
	sge.lkey = memgt->send_mr->lkey;
	
	TEST_NZ(ibv_post_send(qpmgt->qp[qp_id], &wr, &bad_wr));
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

int get_wc( struct ibv_wc *wc )
{
	void *ctx;
	struct ibv_cq *cq;
	int ret;
	while(1){
		ret = ibv_poll_cq(s_ctx->cq_data[0], 1, wc);
		if( ret > 0 ) break;
	}
	if( ret <= 0 || wc->status != IBV_WC_SUCCESS ){
		printf("get CQE fail: %d wr_id: %d\n", wc->status, (int)wc->wr_id);
		return -1;
	}
	//printf("get CQE ok: wr_id: %d\n", (int)wc->wr_id);
	// if( wc->opcode == IBV_WC_SEND ) printf("IBV_WC_SEND\n");
	// if( wc->opcode == IBV_WC_RECV ) printf("IBV_WC_RECV\n");
	// if( wc->opcode == IBV_WC_RDMA_WRITE ) printf("IBV_WC_RDMA_WRITE\n");
	// if( wc->opcode == IBV_WC_RDMA_READ ) printf("IBV_WC_RDMA_READ\n");
#ifdef _TEST_SYN
	return wc->wr_id;
#else
	return 0;
#endif
}

int destroy_qp_management()
{
	for( int i = 0; i < connect_number; i ++ ){
		//printf("waiting %02d\n", i);
		rdma_disconnect(conn_id[i]);
		//fprintf(stderr, "qp: %d state %d\n", i, qp_query(i));
		//fprintf(stderr, "qp: %d num %d\n", i, query_qp_count(qpmgt, i));
		rdma_destroy_qp(conn_id[i]);
		rdma_destroy_id(conn_id[i]);
		//fprintf(stderr, "rdma #%02d disconnect\n", i);
	}
	free(qpmgt); qpmgt = NULL;
	return 0;
}

int destroy_connection()
{
	for( int i = 0; i < cq_data_num; i ++ )
		TEST_NZ(ibv_destroy_cq(s_ctx->cq_data[i]));
	for( int i = 0; i < cq_ctrl_num; i ++ )
		TEST_NZ(ibv_destroy_cq(s_ctx->cq_ctrl[i]));
	TEST_NZ(ibv_destroy_cq(s_ctx->cq_mem[0]));
	free(s_ctx->cq_data); s_ctx->cq_data = NULL;
	free(s_ctx->cq_ctrl); s_ctx->cq_ctrl = NULL;
	TEST_NZ(ibv_destroy_comp_channel(s_ctx->comp_channel));
	TEST_NZ(ibv_destroy_comp_channel(s_ctx->mem_channel));
	TEST_NZ(ibv_dealloc_pd(s_ctx->pd));
	rdma_destroy_event_channel(ec);
	free(s_ctx); s_ctx = NULL;
	return 0;
}

int destroy_memory_management( int end )// 0 active 1 backup
{	
	TEST_NZ(ibv_dereg_mr(memgt->send_mr));
	free(memgt->send_buffer);  memgt->send_buffer = NULL;
	
	TEST_NZ(ibv_dereg_mr(memgt->recv_mr));
	free(memgt->recv_buffer);  memgt->recv_buffer = NULL;
	
	if( end == 0 ){//active
		TEST_NZ(ibv_dereg_mr(memgt->rdma_send_mr));
	}
	else{//backup
		TEST_NZ(ibv_dereg_mr(memgt->rdma_recv_mr));
		free(memgt->rdma_recv_region); memgt->rdma_recv_region = NULL;
	}
	
	
	free(memgt); memgt = NULL;
	return 0;
}

double elapse_sec()
{
    struct timeval current_tv;
    gettimeofday(&current_tv,NULL);
    return (double)(current_tv.tv_sec)*1000000.0+\
	(double)(current_tv.tv_usec);
}
