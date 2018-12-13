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
#define LOG_TWO_SQUARED 0.4804530139182 // ln(2) * ln(2)
#define LOG_TWO 0.6931471805599453 // ln(2)

#define CHECK_BIT_CHAR(c,k)   (c & (1 << (k)))
#define CHECK_BIT(A, k)       (CHECK_BIT_CHAR(A[((k) / 8)], ((k) % 8)))

#if defined (UNIV_OPENMP)
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
	long n;	//size of input set
	uint64_t m; //number of bits
	float p;
	uint64_t k;	//number of hash functions

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

	// The optimal number of bits
	// m = (n * log(1/p)) / log(2)*log(2)
	m = ceil((-n * log(p)) / LOG_TWO_SQUARED);
	k = round(LOG_TWO * m / n);

	pm_bloom->n_bits = m;
	pm_bloom->n_hashes = k;
	pm_bloom->bloom_length = ceil(m / (CHAR_LEN * 1.0));
	
	pm_bloom->bloom = (unsigned char*) calloc(pm_bloom->bloom_length + 1,
			sizeof(char)); //extra 1 byte to ensure no running off the end
	pm_bloom->elements_added = 0;
	pm_bloom->n_false_pos_reads = 0;
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

/*
 * Add a integer to bloom filter
 * pm_bloom [in]: Pointer to the bloom filter
 * key [in]: the input key
 * */
int
pm_bloom_add(
		PMEM_BLOOM*		pm_bloom, 
		uint64_t		key)
		{
	
	uint64_t k;
	uint64_t i;

	//uint64_t to string
	char *skey = (char*) calloc(21, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	sprintf(skey, "%" PRIx64 "", key);

	k = pm_bloom->n_hashes;

	//(1) Compute k hashed value using k hash function	
	uint64_t * hashed_vals = (uint64_t*) calloc(k, sizeof(uint64_t));
	pm_bloom->hash_func(hashed_vals, k, skey);

	//(2) Add the hashed_vals to the bit array
	for (i = 0; i < k; i++) {
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

/*
 * Check a integer to bloom filter
 * pm_bloom [in]: Pointer to the bloom filter
 * key [in]: the input key
 * return: BLOOM_NOT_EXIST or BLOOM_MAY_EXIST
 * */
int
pm_bloom_check(
		PMEM_BLOOM*		pm_bloom,
		uint64_t		key){

	uint64_t k;
	uint64_t i;
	int ret = BLOOM_MAY_EXIST; 

	//uint64_t to string
	char *skey = (char*) calloc(21, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	sprintf(skey, "%" PRIx64 "", key);

	k = pm_bloom->n_hashes;

	//(1) Compute n hashed value using n hash function	
	uint64_t * hashed_vals = (uint64_t*) calloc(k, sizeof(uint64_t));
	pm_bloom->hash_func(hashed_vals, k, skey);
	
	//(2) Check in the bit array
	for (i = 0; i < k; i++) {
		int tmp_check = CHECK_BIT(pm_bloom->bloom,
				(hashed_vals[i] % pm_bloom->n_bits));
		if (tmp_check == 0){
			ret = BLOOM_NOT_EXIST;
			break;
		}
	} //end for

	//(3) Free the temp resources
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
Print statistic information  
 * */
void
pm_bloom_stats(PMEM_BLOOM* bf) {

    printf("BloomFilter\n\
    bits: %" PRIu64 "\n\
    estimated elements: %" PRIu64 "\n\
    number hashes: %d\n\
    max false positive rate: %f\n\
    bloom length (8 bits): %ld\n\
    elements added: %" PRIu64 "\n\
    estimated elements added: %" PRIu64 "\n\
    current false positive reads: %zu\n\
    current false positive rate: %f\n\
    number bits set: %" PRIu64 "\n",
    bf->n_bits,
   	bf->est_elements,
   	bf->n_hashes,
    bf->false_pos_prob,
   	bf->bloom_length,
   	bf->elements_added,
    pm_bloom_est_elements(bf),
   	bf->n_false_pos_reads,
    pm_bloom_current_false_pos_prob(bf),
    pm_bloom_count_set_bits(bf));
}

uint64_t pm_bloom_est_elements(PMEM_BLOOM*	bf) {
	uint64_t m, x, k;
	m = bf->n_bits;
	x = pm_bloom_count_set_bits(bf);
	k = bf->n_hashes;

    double log_n = log(1 - ((double) x / (double) m));
    return (uint64_t)-(((double) m / k) * log_n);

}

uint64_t pm_bloom_count_set_bits(PMEM_BLOOM* bf){
	uint64_t i;
	uint64_t count;

	count = 0;
	for (i = 0; i < bf->bloom_length; i++){
		count += __sum_bits_set_char(bf->bloom[i]);
	}
}
float pm_bloom_current_false_pos_prob(PMEM_BLOOM *bf) {
    int num = (bf->n_hashes * -1 * bf->elements_added);
    double d = num / (float) bf->n_bits;
    double e = exp(d);
    return pow((1 - e), bf->n_hashes);
}
/////////////////////// Counting Bloom Filter ///////////////////////////

PMEM_CBF* 
pm_cbf_alloc(
		uint64_t	est_elements,
		float		false_pos_prob,
		bh_func		hash_func) {
	PMEM_CBF* cbf = NULL;
	long n;	//size of input set
	float p; // false-positive probability
	uint64_t m; //number of counters
	uint64_t k;	//number of hash functions

	uint64_t i;

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

	cbf = (PMEM_CBF*) malloc(sizeof(PMEM_CBF));
	cbf->est_elements = n = est_elements;
	cbf->false_pos_prob = p = false_pos_prob;

	// The optimal number of counters. We treat a counter as a bit in the original Bloom Filter
	// m = (n * log(1/p)) / log(2)*log(2)
	m = ceil((-n * log(p)) / LOG_TWO_SQUARED);
	k = round(LOG_TWO * m / n);

	cbf->n_counts = m;
	cbf->n_hashes = k;
	
	cbf->bloom = (uint16_t*) calloc(m, sizeof(uint16_t));
	for (i = 0; i < m; i++)
		cbf->bloom[i] = 0;
	
	cbf->elements_added = 0;
	cbf->n_false_pos_reads = 0;
	cbf->hash_func = (hash_func == NULL) ? __default_hash : hash_func;

	return cbf;
}

void
pm_cbf_free(PMEM_CBF* cbf) {
	free (cbf->bloom);
	cbf->bloom = NULL;
	
	free (cbf);
	cbf = NULL;
}

/*
 * Add a integer to bloom filter
 * pm_bloom [in]: Pointer to the bloom filter
 * key [in]: the input key
 * */
int
pm_cbf_add(
		PMEM_CBF*		cbf, 
		uint64_t		key) {

	uint64_t k, m;
	uint64_t i;

	char *skey = (char*) calloc(21, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	sprintf(skey, "%" PRIx64 "", key);

	k = cbf->n_hashes;
	m = cbf->n_counts;
	//(1) Compute k hashed value using k hash function	
	uint64_t * hashed_vals = (uint64_t*) calloc(k, sizeof(uint64_t));
	cbf->hash_func(hashed_vals, k, skey);

	//(2) Increase the hashed_vals to the count array
	for (i = 0; i < k; i++){
		uint64_t idx = hashed_vals[i] % m;
		ATOMIC
		cbf->bloom[idx]++;
	}

	ATOMIC
	cbf->elements_added++;

	//(3) Free temp resource
	free (hashed_vals);
	free (skey);
	
	return PMEM_SUCCESS;
}

/*
 * Check a integer to bloom filter
 * pm_bloom [in]: Pointer to the bloom filter
 * key [in]: the input key
 * return: BLOOM_NOT_EXIST or BLOOM_MAY_EXIST
 * */
int
pm_cbf_check(
		PMEM_CBF*		cbf, 
		uint64_t		key) {

	uint64_t k, m;
	uint64_t i;
	int ret = BLOOM_MAY_EXIST; 

	char *skey = (char*) calloc(21, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	sprintf(skey, "%" PRIx64 "", key);

	k = cbf->n_hashes;
	m = cbf->n_counts;
	//(1) Compute k hashed value using k hash function	
	uint64_t * hashed_vals = (uint64_t*) calloc(k, sizeof(uint64_t));
	cbf->hash_func(hashed_vals, k, skey);

	//(2) Check in the bit array
	for (i = 0; i < k; i++) {
		uint64_t idx = hashed_vals[i] % m;
		if (cbf->bloom[idx] == 0){
			ret = BLOOM_NOT_EXIST;
			break;
		}
	} //end for

	//(3) free the temp resources
	free (hashed_vals);
	free (skey);

	return ret;
}

/*
 * Remove a integer from bloom filter
 * pm_bloom [in]: Pointer to the bloom filter
 * key [in]: the input key
 * */
int
pm_cbf_remove(
		PMEM_CBF*		cbf, 
		uint64_t		key) {

	uint64_t k, m;
	uint64_t i;

	char *skey = (char*) calloc(21, sizeof(char)); // largest value is 7FFF FFFF FFFF FFFF	
	sprintf(skey, "%" PRIx64 "", key);

	k = cbf->n_hashes;
	m = cbf->n_counts;
	//(1) Compute k hashed value using k hash function	
	uint64_t * hashed_vals = (uint64_t*) calloc(k, sizeof(uint64_t));
	cbf->hash_func(hashed_vals, k, skey);

	//(2) Decrease the hashed_vals to the count array
	for (i = 0; i < k; i++){
		uint64_t idx = hashed_vals[i] % m;
		assert(cbf->bloom[idx] >= 0);

		ATOMIC
		cbf->bloom[idx]--;
	}

	ATOMIC
	cbf->elements_added--;

	//(3) Free temp resource
	free (hashed_vals);
	free (skey);
	
	return PMEM_SUCCESS;
}

void
pm_cbf_stats(PMEM_CBF* cbf) {

    printf("Counting BloomFilter\n\
    counters: %" PRIu64 " (%f MB) \n\
    estimated elements: %" PRIu64 "\n\
    number hashes: %d\n\
    max false positive rate: %f\n\
    current false positive reads: %zu\n\
    current false positive rate: %f\n\
    elements added: %" PRIu64 "\n",
    cbf->n_counts, cbf->n_counts * sizeof(uint16_t) * 1.0 / (1024*1024),
   	cbf->est_elements,
   	cbf->n_hashes,
    cbf->false_pos_prob,
   	cbf->n_false_pos_reads,
    pm_cbf_current_false_pos_prob(cbf),
   	cbf->elements_added);
}

float pm_cbf_current_false_pos_prob(PMEM_CBF *cbf) {
    int num = (cbf->n_hashes * -1 * cbf->elements_added);
    double d = num / (float) cbf->n_counts;
    double e = exp(d);
    return pow((1 - e), cbf->n_hashes);
}

///////////////////// Internal Functions /////////////
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

static int __sum_bits_set_char(char c) {
    int j, count = 0;
    for (j = 0; j < CHAR_LEN; j++) {
        count += (CHECK_BIT_CHAR(c, j) != 0) ? 1 : 0;
    }
    return count;
}

#endif //UNIV_PMEMOBJ_BLOOM
