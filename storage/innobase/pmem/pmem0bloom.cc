/* 
 * Author; Trong-Dat Nguyen
 * Bloom Filter with  NVDIMM
 * Using libpmemobj
 * Copyright (c) 2018 VLDB Lab - Sungkyunkwan University
 * Credit: Tyler Barrus  for the basic implement on disk
 * */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>                                                                      
#include <stdint.h> //for uint64_t
#include <assert.h>
#include <wchar.h>
#include <unistd.h> //for access()

#include "my_pmem_common.h"
#include "my_pmemobj.h"

#if defined (UNIV_PMEMOBJ_BLOOM)
/* define some constant magic looking numbers */
#define CHAR_LEN 8
#define LOG_TWO_SQUARED 0.4804530139182
#define LOG_TWO 0.6931471805599453

#define CHECK_BIT_CHAR(c,k)   (c & (1 << (k)))
#define CHECK_BIT(A, k)       (CHECK_BIT_CHAR(A[((k) / 8)], ((k) % 8)))

#if defined (_OPENMP)
#define ATOMIC _Pragma ("omp atomic")
#define CRITICAL _Pragma ("omp critical (bloom_filter_critical)")
#else
#define ATOMIC
#define CRITICAL
#endif

/*
 * Allocate the bloom filter in PMEM
 * est_elements: number of estimated elements
 * */
PMEM_BLOOM* 
pm_bloom_alloc(
		uint64_t	est_elements,
		float		false_pos_prob,
		bh_func		hash_func) {
//PMEM_BLOOM* 
//pm_bloom_alloc(
//		uint64_t	est_elements,
//		float		false_pos_prob
//		) {
	PMEM_BLOOM* pm_bloom = NULL;
	uint64_t n;	
	float p;
	uint64_t k;	

	// Check the input params
	bool is_input_error =(
	   	est_elements <= 0 ||
		est_elements > UINT64_MAX ||
		false_pos_prob <= 0.0 ||
		false_pos_prob >= 1.0
		);
	if (is_input_error){
		printf("PMEM_ERROR: The input params of pm_bloom_alloc() has error, check again!\n");
		assert(0);
	}

	pm_bloom = (PMEM_BLOOM*) malloc(sizeof(PMEM_BLOOM));
	pm_bloom->est_elements = n = est_elements;
	pm_bloom->false_pos_prob = p = false_pos_prob;
	
	pm_bloom->n_bits = ceil((-n * log(p)) / LOG_TWO_SQUARED);
	pm_bloom->n_hashes = round(LOG_TWO * pm_bloom->n_bits / n);
	pm_bloom->bloom_length = ceil(pm_bloom->n_bits / (CHAR_LEN * 1.0));
	
	pm_bloom->bloom = (unsigned char*) calloc(pm_bloom->bloom_length + 1,
			sizeof(char)); //extra 1 byte to ensure no running off the end
	pm_bloom->elements_added = 0;
	pm_bloom->hash_func = (hash_func == NULL) ? __default_hash : hash_func;

	return pm_bloom;
}

void
pm_bloom_free(PMEM_BLOOM* pm_bloom){
	free (pm_bloom->bloom);
	pm_bloom->bloom = NULL;

	free (pm_bloom);
	pm_bloom = NULL;
}

int
pm_bloom_add(
		PMEM_BLOOM*		pm_bloom, 
		uint64_t		key)
		{
	
	uint64_t n;
	uint64_t i;

	//uint64_t to string
	char *skey = (char*) calloc(17, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	sprintf(skey, "%" PRIx64 "", key);

	n = pm_bloom->n_hashes;

	//(1) Compute n hashed value using n hash function	
	uint64_t * hashed_vals = (uint64_t*) calloc(n, sizeof(uint64_t));
	pm_bloom->hash_func(hashed_vals, n, skey);

	//(2) Add the hashed_vals to the bit array
	for (i = 0; i < n; i++) {
		//Atomicaly set the bit
		ATOMIC
		pm_bloom->bloom[ (hashed_vals[i] % pm_bloom->n_bits) / 8] |= (1 << ( (hashed_vals[i] % pm_bloom->n_bits) % 8));
	}
	
	ATOMIC
	pm_bloom->elements_added++;
	//(3) Free temp resource
	
	free (hashed_vals);
	free (skey);

	return PMEM_SUCCESS;
}

int
pm_bloom_check(
		PMEM_BLOOM*		pm_bloom,
		uint64_t		key){

	uint64_t n;
	uint64_t i;
	int ret = BLOOM_MAY_EXIST; 

	//uint64_t to string
	char *skey = (char*) calloc(17, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	sprintf(skey, "%" PRIx64 "", key);

	n = pm_bloom->n_hashes;

	//(1) Compute n hashed value using n hash function	
	uint64_t * hashed_vals = (uint64_t*) calloc(n, sizeof(uint64_t));
	pm_bloom->hash_func(hashed_vals, n, skey);
	
	//(2) Check in the bit array
	for (i = 0; i < n; i++) {
		int tmp_check = CHECK_BIT(pm_bloom->bloom,
				(hashed_vals[i] % pm_bloom->n_bits));
		if (tmp_check == 0){
			ret = BLOOM_NOT_EXIST;
			break;
		}
	} //end for

	//(3) Free temp resource
	free (hashed_vals);
	free (skey);

	return ret;
}
/*
 * Get the number of bits set to 1
 * */
uint64_t
pm_bloom_get_set_bits(
		PMEM_BLOOM*		pm_bloom) {
	uint64_t count = 0;

	return count;
}
/*
 * The default hash function
 * n_hashes [in]: the number of hash functions
 * skey: the input key
 * return an array of hashed values with lenght equal to n_hashes
 * Note: The hashed_vals is allocated and free by the caller
 * */
void
__default_hash(
		uint64_t* hashed_vals,
		uint64_t n_hashes,
		char* str) {

	uint64_t i;

	int tem_n = 17; //16 bytes + 1 for padding

	char *stem = (char*) calloc(tem_n, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	
	//compute hash values	
	for (i = 0; i < n_hashes; i++) {
		if (i == 0){
			//The input of first value is the key itself
			hashed_vals[i] = __fnv_1a(str);
		} else {
			//From the second value, the input is the previous value
			uint64_t prev = hashed_vals[i-1];
			memset(stem, 0, tem_n);
            sprintf(stem, "%" PRIx64 "", prev);
			hashed_vals[i] = __fnv_1a(stem);
		}
	}
	free (stem);
}
/*
 *fnv_1a hash function
    // FNV-1a hash (http://www.isthe.com/chongo/tech/comp/fnv/)
 * */
uint64_t __fnv_1a (char* key) {
    int i;
	int len = strlen(key);
    uint64_t h = 14695981039346656073ULL; // FNV_OFFSET 64 bit
    for (i = 0; i < len; i++){
            h = h ^ (unsigned char) key[i];
            h = h * 1099511628211ULL; // FNV_PRIME 64 bit
    }
    return h;
}

#endif //UNIV_PMEMOBJ_BLOOM
