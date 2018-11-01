
#ifndef __PMEM_COMMON_H__
#define __PMEM_COMMON_H__


#define PMEM_MAX_FILES 1000
#define PMEM_MAX_FILE_NAME_LENGTH 10000
#define PMEM_HASH_MASK 1653893711

#define PMEM_ID_NONE -1 //ID not defined

//random number for checking AIO
#define PMEM_AIO_CHECK 7988

//wait for a aio write, call when there is no free block
#define PMEM_WAIT_FOR_WRITE 200
#define PMEM_WAIT_FOR_FREE_LIST 10000

//error handler
#define PMEM_SUCCESS 0
#define PMEM_ERROR -1

#define TOID_ARRAY(x) TOID(x)

#define PMEMOBJ_FILE_NAME "pmemobjfile"
//OS_FILE_LOG_BLOCK_SIZE =512 is defined in os0file.h
static const size_t PMEM_MB = 1024 * 1024;
static const size_t PMEM_MAX_LOG_BUF_SIZE = 1 * 1024 * PMEM_MB;
static const size_t PMEM_PAGE_SIZE = 16*1024; //16KB
static const size_t PMEM_MAX_DBW_PAGES= 128; // 2 * extent_size

//#define PMEM_N_BUCKETS 128 
//#define PMEM_USED_FREE_RATIO 0.2
#define PMEM_MAX_LISTS_PER_BUCKET 2
//#define PMEM_BUF_THRESHOLD 0.8


enum {
	PMEM_READ = 1,
	PMEM_WRITE = 2
};
enum PMEM_OBJ_TYPES {
	UNKNOWN_TYPE,
	LOG_BUF_TYPE,
	DBW_TYPE,
	BUF_TYPE,
	META_DATA_TYPE
};
enum PMEM_BLOCK_STATE {
    PMEM_FREE_BLOCK = 1,
    PMEM_IN_USED_BLOCK = 2,
    PMEM_IN_FLUSH_BLOCK=3
};
enum pm_list_cleaner_state {
	/** Not requested any yet.
	Moved from FINISHED by the coordinator. */
	LIST_CLEANER_STATE_NONE = 0,
	/** Requested but not started flushing.
	Moved from NONE by the coordinator. */
	LIST_CLEANER_STATE_REQUESTED,
	/** Flushing is on going.
	Moved from REQUESTED by the worker. */
	LIST_CLEANER_STATE_FLUSHING,
	/** Flushing was finished.
	Moved from FLUSHING by the worker. */
	LIST_CLEANER_STATE_FINISHED
};

//struct __pmem_aio_param;
//typedef struct __pmem_aio_param PMEM_AIO_PARAM;
//
//struct __pmem_aio_param {
//	const char* name;
//	void*		file;	
//	void*		buf;
//	uint64_t	offset;
//	uint64_t		n;	
//	void*		m1;
//	void*		m2;
//};

static inline int file_exists(char const *file);

/*
 *  * file_exists -- checks if file exists
 *   */
static inline int file_exists(char const *file)
{
	    return access(file, F_OK);
}

#endif
