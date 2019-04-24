/*****************************************************************************

Copyright (c) 1997, 2017, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2012, Facebook Inc.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file log/log0recv.cc
Recovery

Created 9/20/1997 Heikki Tuuri
*******************************************************/

#include "ha_prototypes.h"

#include <vector>
#include <map>
#include <string>

#include "log0recv.h"

#ifdef UNIV_NONINL
#include "log0recv.ic"
#endif

#include <my_aes.h>

#include "mem0mem.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "mtr0mtr.h"
#include "mtr0log.h"
#include "page0cur.h"
#include "page0zip.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "ibuf0ibuf.h"
#include "trx0undo.h"
#include "trx0rec.h"
#include "fil0fil.h"
#include "fsp0sysspace.h"
#include "ut0new.h"
#include "row0trunc.h"
#ifndef UNIV_HOTBACKUP
# include "buf0rea.h"
# include "srv0srv.h"
# include "srv0start.h"
# include "trx0roll.h"
# include "row0merge.h"
#else /* !UNIV_HOTBACKUP */
/** This is set to false if the backup was originally taken with the
mysqlbackup --include regexp option: then we do not want to create tables in
directories which were not included */
bool	recv_replay_file_ops	= true;
#include "fut0lst.h"
#endif /* !UNIV_HOTBACKUP */

#if defined(UNIV_PMEMOBJ_LOG) || defined (UNIV_PMEMOBJ_WAL) || defined (UNIV_PMEMOBJ_PART_PL)
#include "my_pmemobj.h"
extern PMEM_WRAPPER* gb_pmw;

//static bool IS_GLOBAL_HASHTABLE = true;
static bool IS_GLOBAL_HASHTABLE = false;
#endif


/** Log records are stored in the hash table in chunks at most of this size;
this must be less than UNIV_PAGE_SIZE as it is stored in the buffer pool */
#define RECV_DATA_BLOCK_SIZE	(MEM_MAX_ALLOC_IN_BUF - sizeof(recv_data_t))

/** Read-ahead area in applying log records to file pages */
#define RECV_READ_AHEAD_AREA	32

/** The recovery system */
recv_sys_t*	recv_sys = NULL;
/** TRUE when applying redo log records during crash recovery; FALSE
otherwise.  Note that this is FALSE while a background thread is
rolling back incomplete transactions. */
volatile bool	recv_recovery_on;

#ifndef UNIV_HOTBACKUP
/** TRUE when recv_init_crash_recovery() has been called. */
bool	recv_needed_recovery;
#else
# define recv_needed_recovery			false
# define buf_pool_get_curr_size() (5 * 1024 * 1024)
#endif /* !UNIV_HOTBACKUP */
# ifdef UNIV_DEBUG
/** TRUE if writing to the redo log (mtr_commit) is forbidden.
Protected by log_sys->mutex. */
bool	recv_no_log_write = false;
# endif /* UNIV_DEBUG */

/** TRUE if buf_page_is_corrupted() should check if the log sequence
number (FIL_PAGE_LSN) is in the future.  Initially FALSE, and set by
recv_recovery_from_checkpoint_start(). */
bool	recv_lsn_checks_on;

/** If the following is TRUE, the buffer pool file pages must be invalidated
after recovery and no ibuf operations are allowed; this becomes TRUE if
the log record hash table becomes too full, and log records must be merged
to file pages already before the recovery is finished: in this case no
ibuf operations are allowed, as they could modify the pages read in the
buffer pool before the pages have been recovered to the up-to-date state.

TRUE means that recovery is running and no operations on the log files
are allowed yet: the variable name is misleading. */
#ifndef UNIV_HOTBACKUP
bool	recv_no_ibuf_operations;
/** TRUE when the redo log is being backed up */
# define recv_is_making_a_backup		false
/** TRUE when recovering from a backed up redo log file */
# define recv_is_from_backup			false
#else /* !UNIV_HOTBACKUP */
/** true if the backup is an offline backup */
volatile bool is_online_redo_copy = true;
/**true if the last flushed lsn read at the start of backup */
volatile lsn_t backup_redo_log_flushed_lsn;

/** TRUE when the redo log is being backed up */
bool	recv_is_making_a_backup	= false;
/** TRUE when recovering from a backed up redo log file */
bool	recv_is_from_backup	= false;
# define buf_pool_get_curr_size() (5 * 1024 * 1024)
#endif /* !UNIV_HOTBACKUP */
/** The following counter is used to decide when to print info on
log scan */
static ulint	recv_scan_print_counter;

/** The type of the previous parsed redo log record */
static mlog_id_t	recv_previous_parsed_rec_type;
/** The offset of the previous parsed redo log record */
static ulint	recv_previous_parsed_rec_offset;
/** The 'multi' flag of the previous parsed redo log record */
static ulint	recv_previous_parsed_rec_is_multi;

/** This many frames must be left free in the buffer pool when we scan
the log and store the scanned log records in the buffer pool: we will
use these free frames to read in pages when we start applying the
log records to the database.
This is the default value. If the actual size of the buffer pool is
larger than 10 MB we'll set this value to 512. */
ulint	recv_n_pool_free_frames;

/** The maximum lsn we see for a page during the recovery process. If this
is bigger than the lsn we are able to scan up to, that is an indication that
the recovery failed and the database may be corrupt. */
lsn_t	recv_max_page_lsn;

#ifdef UNIV_PFS_THREAD
mysql_pfs_key_t	trx_rollback_clean_thread_key;
#endif /* UNIV_PFS_THREAD */

#ifndef UNIV_HOTBACKUP
# ifdef UNIV_PFS_THREAD
mysql_pfs_key_t	recv_writer_thread_key;
# endif /* UNIV_PFS_THREAD */

/** Flag indicating if recv_writer thread is active. */
volatile bool	recv_writer_thread_active = false;
#endif /* !UNIV_HOTBACKUP */

#ifndef	DBUG_OFF
/** Return string name of the redo log record type.
@param[in]	type	record log record enum
@return string name of record log record */
const char*
get_mlog_string(mlog_id_t type);
#endif /* !DBUG_OFF */

/* prototypes */

#ifndef UNIV_HOTBACKUP
/*******************************************************//**
Initialize crash recovery environment. Can be called iff
recv_needed_recovery == false. */
static
void
recv_init_crash_recovery(void);
/*===========================*/
#endif /* !UNIV_HOTBACKUP */

/** Tablespace item during recovery */
struct file_name_t {
	/** Tablespace file name (MLOG_FILE_NAME) */
	std::string	name;
	/** Tablespace object (NULL if not valid or not found) */
	fil_space_t*	space;
	/** Whether the tablespace has been deleted */
	bool		deleted;

	/** Constructor */
	file_name_t(std::string name_, bool deleted_) :
		name(name_), space(NULL), deleted (deleted_) {}
};

/** Map of dirty tablespaces during recovery */
typedef std::map<
	ulint,
	file_name_t,
	std::less<ulint>,
	ut_allocator<std::pair<const ulint, file_name_t> > >	recv_spaces_t;

static recv_spaces_t	recv_spaces;

/** Process a file name from a MLOG_FILE_* record.
@param[in,out]	name		file name
@param[in]	len		length of the file name
@param[in]	space_id	the tablespace ID
@param[in]	deleted		whether this is a MLOG_FILE_DELETE record
@retval true if able to process file successfully.
@retval false if unable to process the file */
static
bool
fil_name_process(
	char*	name,
	ulint	len,
	ulint	space_id,
	bool	deleted)
{
	bool	processed = true;

	/* We will also insert space=NULL into the map, so that
	further checks can ensure that a MLOG_FILE_NAME record was
	scanned before applying any page records for the space_id. */

	os_normalize_path(name);
	file_name_t	fname(std::string(name, len - 1), deleted);
	std::pair<recv_spaces_t::iterator,bool> p = recv_spaces.insert(
		std::make_pair(space_id, fname));
	ut_ad(p.first->first == space_id);

	file_name_t&	f = p.first->second;

	if (deleted) {
		/* Got MLOG_FILE_DELETE */

		if (!p.second && !f.deleted) {
			f.deleted = true;
			if (f.space != NULL) {
				fil_space_free(space_id, false);
				f.space = NULL;
			}
		}

		ut_ad(f.space == NULL);
	} else if (p.second // the first MLOG_FILE_NAME or MLOG_FILE_RENAME2
		   || f.name != fname.name) {
		fil_space_t*	space;

		/* Check if the tablespace file exists and contains
		the space_id. If not, ignore the file after displaying
		a note. Abort if there are multiple files with the
		same space_id. */
		switch (fil_ibd_load(space_id, name, space)) {
		case FIL_LOAD_OK:
			ut_ad(space != NULL);

			/* For encrypted tablespace, set key and iv. */
			if (FSP_FLAGS_GET_ENCRYPTION(space->flags)
			    && recv_sys->encryption_list != NULL) {
				dberr_t				err;
				encryption_list_t::iterator	it;

				for (it = recv_sys->encryption_list->begin();
				     it != recv_sys->encryption_list->end();
				     it++) {
					if (it->space_id == space->id) {
						err = fil_set_encryption(
							space->id,
							Encryption::AES,
							it->key,
							it->iv);
						if (err != DB_SUCCESS) {
							ib::error()
								<< "Can't set"
								" encryption"
								" information"
								" for"
								" tablespace"
								<< space->name
								<< "!";
						}
						ut_free(it->key);
						ut_free(it->iv);
						it->key = NULL;
						it->iv = NULL;
						it->space_id = 0;
					}
				}
			}

			if (f.space == NULL || f.space == space) {
				f.name = fname.name;
				f.space = space;
				f.deleted = false;
			} else {
				ib::error() << "Tablespace " << space_id
					<< " has been found in two places: '"
					<< f.name << "' and '" << name << "'."
					" You must delete one of them.";
				recv_sys->found_corrupt_fs = true;
				processed = false;
			}
			break;

		case FIL_LOAD_ID_CHANGED:
			ut_ad(space == NULL);
			break;

		case FIL_LOAD_NOT_FOUND:
			/* No matching tablespace was found; maybe it
			was renamed, and we will find a subsequent
			MLOG_FILE_* record. */
			ut_ad(space == NULL);

			if (srv_force_recovery) {
				/* Without innodb_force_recovery,
				missing tablespaces will only be
				reported in
				recv_init_crash_recovery_spaces().
				Enable some more diagnostics when
				forcing recovery. */

				ib::info()
					<< "At LSN: " << recv_sys->recovered_lsn
					<< ": unable to open file " << name
					<< " for tablespace " << space_id;
			}
			break;

		case FIL_LOAD_INVALID:
			ut_ad(space == NULL);
			if (srv_force_recovery == 0) {
#ifndef UNIV_HOTBACKUP
				ib::warn() << "We do not continue the crash"
					" recovery, because the table may"
					" become corrupt if we cannot apply"
					" the log records in the InnoDB log to"
					" it. To fix the problem and start"
					" mysqld:";
				ib::info() << "1) If there is a permission"
					" problem in the file and mysqld"
					" cannot open the file, you should"
					" modify the permissions.";
				ib::info() << "2) If the tablespace is not"
					" needed, or you can restore an older"
					" version from a backup, then you can"
					" remove the .ibd file, and use"
					" --innodb_force_recovery=1 to force"
					" startup without this file.";
				ib::info() << "3) If the file system or the"
					" disk is broken, and you cannot"
					" remove the .ibd file, you can set"
					" --innodb_force_recovery.";
				recv_sys->found_corrupt_fs = true;
#else
				ib::warn() << "We do not continue the apply-log"
					" operation because the tablespace may"
					" become corrupt if we cannot apply"
					" the log records in the redo log"
					" records to it.";
#endif /* !UNIV_BACKUP  */
				processed = false;
				break;
			}

			ib::info() << "innodb_force_recovery was set to "
				<< srv_force_recovery << ". Continuing crash"
				" recovery even though we cannot access the"
				" files for tablespace " << space_id << ".";
			break;
		}
	}
	return(processed);
}

#ifndef UNIV_HOTBACKUP
/** Parse or process a MLOG_FILE_* record.
@param[in]	ptr		redo log record
@param[in]	end		end of the redo log buffer
@param[in]	space_id	the tablespace ID
@param[in]	first_page_no	first page number in the file
@param[in]	type		MLOG_FILE_NAME or MLOG_FILE_DELETE
or MLOG_FILE_CREATE2 or MLOG_FILE_RENAME2
@return pointer to next redo log record
@retval NULL if this log record was truncated */
static
byte*
fil_name_parse(
	byte*		ptr,
	const byte*	end,
	ulint		space_id,
	ulint		first_page_no,
	mlog_id_t	type)
{
	if (type == MLOG_FILE_CREATE2) {
		if (end < ptr + 4) {
			return(NULL);
		}
		ptr += 4;
	}

	if (end < ptr + 2) {
		return(NULL);
	}

	ulint	len = mach_read_from_2(ptr);
	ptr += 2;
	if (end < ptr + len) {
		return(NULL);
	}

	/* MLOG_FILE_* records should only be written for
	user-created tablespaces. The name must be long enough
	and end in .ibd. */
	bool corrupt = is_predefined_tablespace(space_id)
		|| first_page_no != 0 // TODO: multi-file user tablespaces
		|| len < sizeof "/a.ibd\0"
		|| memcmp(ptr + len - 5, DOT_IBD, 5) != 0
		|| memchr(ptr, OS_PATH_SEPARATOR, len) == NULL;

	byte*	end_ptr	= ptr + len;

	switch (type) {
	default:
		ut_ad(0); // the caller checked this
	case MLOG_FILE_NAME:
		if (corrupt) {
			recv_sys->found_corrupt_log = true;
			break;
		}

		fil_name_process(
			reinterpret_cast<char*>(ptr), len, space_id, false);
		break;
	case MLOG_FILE_DELETE:
		if (corrupt) {
			recv_sys->found_corrupt_log = true;
			break;
		}

		fil_name_process(
			reinterpret_cast<char*>(ptr), len, space_id, true);

		break;
	case MLOG_FILE_CREATE2:
		break;
	case MLOG_FILE_RENAME2:
		if (corrupt) {
			recv_sys->found_corrupt_log = true;
		}

		/* The new name follows the old name. */
		byte*	new_name = end_ptr + 2;
		if (end < new_name) {
			return(NULL);
		}

		ulint	new_len = mach_read_from_2(end_ptr);

		if (end < end_ptr + 2 + new_len) {
			return(NULL);
		}

		end_ptr += 2 + new_len;

		corrupt = corrupt
			|| new_len < sizeof "/a.ibd\0"
			|| memcmp(new_name + new_len - 5, DOT_IBD, 5) != 0
			|| !memchr(new_name, OS_PATH_SEPARATOR, new_len);

		if (corrupt) {
			recv_sys->found_corrupt_log = true;
			break;
		}

		fil_name_process(
			reinterpret_cast<char*>(ptr), len,
			space_id, false);
		fil_name_process(
			reinterpret_cast<char*>(new_name), new_len,
			space_id, false);

		if (!fil_op_replay_rename(
			    space_id, first_page_no,
			    reinterpret_cast<const char*>(ptr),
			    reinterpret_cast<const char*>(new_name))) {
			recv_sys->found_corrupt_fs = true;
		}
	}

	return(end_ptr);
}
#else /* !UNIV_HOTBACKUP */
/** Parse a file name retrieved from a MLOG_FILE_* record,
and return the absolute file path corresponds to backup dir
as well as in the form of database/tablespace
@param[in]	file_name		path emitted by the redo log
@param[out]	absolute_path	absolute path of tablespace
corresponds to backup dir
@param[out]	tablespace_name	name in the form of database/table */
static
void
make_abs_file_path(
	const std::string&	name,
	std::string&		absolute_path,
	std::string&		tablespace_name)
{
	std::string file_name = name;
	std::string path = fil_path_to_mysql_datadir;
	size_t pos = std::string::npos;

	if (is_absolute_path(file_name.c_str())) {

		pos = file_name.rfind(OS_PATH_SEPARATOR);
		std::string temp_name = file_name.substr(0, pos);
		pos = temp_name.rfind(OS_PATH_SEPARATOR);
		++pos;
		file_name = file_name.substr(pos, file_name.length());
		path += OS_PATH_SEPARATOR + file_name;
	} else {
		pos = file_name.find(OS_PATH_SEPARATOR);
		++pos;
		file_name = file_name.substr(pos, file_name.length());
		path += OS_PATH_SEPARATOR + file_name;
	}

	absolute_path = path;

	/* remove the .ibd extension */
	pos = file_name.rfind(".ibd");
	if (pos != std::string::npos)
		tablespace_name = file_name.substr(0, pos);

	/* space->name uses '/', not OS_PATH_SEPARATOR,
	update the seperator */
	if (OS_PATH_SEPARATOR != '/') {
		pos = tablespace_name.find(OS_PATH_SEPARATOR);
		while (pos != std::string::npos) {
			tablespace_name[pos] = '/';
			pos = tablespace_name.find(OS_PATH_SEPARATOR);
		}
	}

}

/** Wrapper around fil_name_process()
@param[in]	name		absolute path of tablespace file
@param[in]	space_id	the tablespace ID
@retval		true		if able to process file successfully.
@retval		false		if unable to process the file */
bool
fil_name_process(
	const char*	name,
	ulint	space_id)
{
	size_t length = strlen(name);
	++length;

	char* file_name = static_cast<char*>(ut_malloc_nokey(length));
	strncpy(file_name, name,length);

	bool processed = fil_name_process(file_name, length, space_id, false);

	ut_free(file_name);
	return(processed);
}

/** Parse or process a MLOG_FILE_* record.
@param[in]	ptr		redo log record
@param[in]	end		end of the redo log buffer
@param[in]	space_id	the tablespace ID
@param[in]	first_page_no	first page number in the file
@param[in]	type		MLOG_FILE_NAME or MLOG_FILE_DELETE
or MLOG_FILE_CREATE2 or MLOG_FILE_RENAME2
@retval	pointer to next redo log record
@retval	NULL if this log record was truncated */
static
byte*
fil_name_parse(
	byte*		ptr,
	const byte*	end,
	ulint		space_id,
	ulint		first_page_no,
	mlog_id_t	type)
{

	ulint flags = mach_read_from_4(ptr);

	if (type == MLOG_FILE_CREATE2) {
		if (end < ptr + 4) {
			return(NULL);
		}
		ptr += 4;
	}

	if (end < ptr + 2) {
		return(NULL);
	}

	ulint	len = mach_read_from_2(ptr);
	ptr += 2;
	if (end < ptr + len) {
		return(NULL);
	}

	os_normalize_path(reinterpret_cast<char*>(ptr));

	/* MLOG_FILE_* records should only be written for
	user-created tablespaces. The name must be long enough
	and end in .ibd. */
	bool corrupt = is_predefined_tablespace(space_id)
		|| first_page_no != 0 // TODO: multi-file user tablespaces
		|| len < sizeof "/a.ibd\0"
		|| memcmp(ptr + len - 5, DOT_IBD, 5) != 0
		|| memchr(ptr, OS_PATH_SEPARATOR, len) == NULL;

	byte*	end_ptr = ptr + len;

	if (corrupt) {
		recv_sys->found_corrupt_log = true;
		return(end_ptr);
	}

	std::string abs_file_path, tablespace_name;
	char* name = reinterpret_cast<char*>(ptr);
	char* new_name = NULL;
	recv_spaces_t::iterator itr;

	make_abs_file_path(name, abs_file_path, tablespace_name);

	if (!recv_is_making_a_backup) {

		name = static_cast<char*>(ut_malloc_nokey(
			(abs_file_path.length() + 1)));
		strcpy(name, abs_file_path.c_str());
		len = strlen(name) + 1;
	}
	switch (type) {
	default:
		ut_ad(0); // the caller checked this
	case MLOG_FILE_NAME:
		/* Don't validate tablespaces while copying redo logs
		because backup process might keep some tablespace handles
		open in server datadir.
		Maintain "map of dirty tablespaces" so that assumptions
		for other redo log records are not broken even for dirty
		tablespaces during apply log */
		if (!recv_is_making_a_backup) {
			recv_spaces.insert(std::make_pair(space_id,
						file_name_t(abs_file_path,
						false)));
		}
		break;
	case MLOG_FILE_DELETE:
		/* Don't validate tablespaces while copying redo logs
		because backup process might keep some tablespace handles
		open in server datadir. */
		if (recv_is_making_a_backup)
			break;

		fil_name_process(
			name, len, space_id, true);

		if (recv_replay_file_ops
			&& fil_space_get(space_id)) {
			dberr_t	err = fil_delete_tablespace(
				space_id, BUF_REMOVE_FLUSH_NO_WRITE);
			ut_a(err == DB_SUCCESS);
		}

		break;
	case MLOG_FILE_CREATE2:
		if (recv_is_making_a_backup
		    || (!recv_replay_file_ops)
		    || (is_intermediate_file(abs_file_path.c_str()))
		    || (fil_space_get(space_id))
		    || (fil_space_get_id_by_name(
				tablespace_name.c_str()) != ULINT_UNDEFINED)) {
			/* Don't create table while :-
			1. scanning the redo logs during backup
			2. apply-log on a partial backup
			3. if it is intermediate file
			4. tablespace is already loaded in memory */
		} else {
			itr = recv_spaces.find(space_id);
			if (itr == recv_spaces.end()
				|| (itr->second.name != abs_file_path)) {

				dberr_t ret = fil_ibd_create(
					space_id, tablespace_name.c_str(),
					abs_file_path.c_str(),
					flags, FIL_IBD_FILE_INITIAL_SIZE);

				if (ret != DB_SUCCESS) {
					ib::fatal() << "Could not create the"
						<< " tablespace : "
						<< abs_file_path
						<< " with space Id : "
						<< space_id;
				}
			}
		}
		break;
	case MLOG_FILE_RENAME2:
		/* The new name follows the old name. */
		byte*	new_table_name = end_ptr + 2;
		if (end < new_table_name) {
			return(NULL);
		}

		ulint	new_len = mach_read_from_2(end_ptr);

		if (end < end_ptr + 2 + new_len) {
			return(NULL);
		}

		end_ptr += 2 + new_len;

		char* new_table = reinterpret_cast<char*>(new_table_name);
		os_normalize_path(new_table);

		corrupt = corrupt
			|| new_len < sizeof "/a.ibd\0"
			|| memcmp(new_table_name + new_len - 5, DOT_IBD, 5) != 0
			|| !memchr(new_table_name, OS_PATH_SEPARATOR, new_len);

		if (corrupt) {
			recv_sys->found_corrupt_log = true;
			break;
		}

		if (recv_is_making_a_backup
		    || (!recv_replay_file_ops)
		    || (is_intermediate_file(name))
		    || (is_intermediate_file(new_table))) {
			/* Don't rename table while :-
			1. scanning the redo logs during backup
			2. apply-log on a partial backup
			3. The new name is already used.
			4. A tablespace is not open in memory with the old name.
			This will prevent unintended renames during recovery. */
			break;
		} else {
			make_abs_file_path(new_table, abs_file_path,
					   tablespace_name);

			new_name = static_cast<char*>(ut_malloc_nokey(
				(abs_file_path.length() + 1)));
			strcpy(new_name, abs_file_path.c_str());
			new_len = strlen(new_name) + 1;
		}

		fil_name_process(name, len, space_id, false);
		fil_name_process( new_name, new_len, space_id, false);

		if (!fil_op_replay_rename(
			space_id, first_page_no,
			name,
			new_name)) {
			recv_sys->found_corrupt_fs = true;
		}
	}

	if (!recv_is_making_a_backup) {
		ut_free(name);
		ut_free(new_name);
	}
	return(end_ptr);
}
#endif /* UNIV_HOTBACKUP */

/********************************************************//**
Creates the recovery system. */
void
recv_sys_create(void)
/*=================*/
{
	if (recv_sys != NULL) {

		return;
	}

	recv_sys = static_cast<recv_sys_t*>(ut_zalloc_nokey(sizeof(*recv_sys)));

	mutex_create(LATCH_ID_RECV_SYS, &recv_sys->mutex);
	mutex_create(LATCH_ID_RECV_WRITER, &recv_sys->writer_mutex);

	recv_sys->heap = NULL;
	recv_sys->addr_hash = NULL;
}

/********************************************************//**
Release recovery system mutexes. */
void
recv_sys_close(void)
/*================*/
{
	if (recv_sys != NULL) {
		if (recv_sys->addr_hash != NULL) {
			hash_table_free(recv_sys->addr_hash);
		}

		if (recv_sys->heap != NULL) {
			mem_heap_free(recv_sys->heap);
		}
#ifndef UNIV_HOTBACKUP
		if (recv_sys->flush_start != NULL) {
			os_event_destroy(recv_sys->flush_start);
		}

		if (recv_sys->flush_end != NULL) {
			os_event_destroy(recv_sys->flush_end);
		}
#endif /* !UNIV_HOTBACKUP */
		ut_free(recv_sys->buf);
		ut_free(recv_sys->last_block_buf_start);

#ifndef UNIV_HOTBACKUP
		ut_ad(!recv_writer_thread_active);
		mutex_free(&recv_sys->writer_mutex);
#endif /* !UNIV_HOTBACKUP */

		mutex_free(&recv_sys->mutex);

		ut_free(recv_sys);
		recv_sys = NULL;
	}

	recv_spaces.clear();
}

/********************************************************//**
Frees the recovery system memory. */
void
recv_sys_mem_free(void)
/*===================*/
{
	if (recv_sys != NULL) {
		if (recv_sys->addr_hash != NULL) {
			hash_table_free(recv_sys->addr_hash);
		}

		if (recv_sys->heap != NULL) {
			mem_heap_free(recv_sys->heap);
		}
#ifndef UNIV_HOTBACKUP
		if (recv_sys->flush_start != NULL) {
			os_event_destroy(recv_sys->flush_start);
		}

		if (recv_sys->flush_end != NULL) {
			os_event_destroy(recv_sys->flush_end);
		}
#endif /* !UNIV_HOTBACKUP */
		ut_free(recv_sys->buf);
		ut_free(recv_sys->last_block_buf_start);
		ut_free(recv_sys);
		recv_sys = NULL;
	}
}

#ifndef UNIV_HOTBACKUP
/************************************************************
Reset the state of the recovery system variables. */
void
recv_sys_var_init(void)
/*===================*/
{
	recv_recovery_on = false;
	recv_needed_recovery = false;
	recv_lsn_checks_on = false;
	recv_no_ibuf_operations = false;
	recv_scan_print_counter	= 0;
	recv_previous_parsed_rec_type = MLOG_SINGLE_REC_FLAG;
	recv_previous_parsed_rec_offset	= 0;
	recv_previous_parsed_rec_is_multi = 0;
	recv_n_pool_free_frames	= 256;
	recv_max_page_lsn = 0;
}

/******************************************************************//**
recv_writer thread tasked with flushing dirty pages from the buffer
pools.
@return a dummy parameter */
extern "C"
os_thread_ret_t
DECLARE_THREAD(recv_writer_thread)(
/*===============================*/
	void*	arg MY_ATTRIBUTE((unused)))
			/*!< in: a dummy parameter required by
			os_thread_create */
{
	my_thread_init();
	ut_ad(!srv_read_only_mode);

#ifdef UNIV_PFS_THREAD
	pfs_register_thread(recv_writer_thread_key);
#endif /* UNIV_PFS_THREAD */

#ifdef UNIV_DEBUG_THREAD_CREATION
	ib::info() << "recv_writer thread running, id "
		<< os_thread_pf(os_thread_get_curr_id());
#endif /* UNIV_DEBUG_THREAD_CREATION */

	recv_writer_thread_active = true;

	while (srv_shutdown_state == SRV_SHUTDOWN_NONE) {

		os_thread_sleep(100000);

		mutex_enter(&recv_sys->writer_mutex);

		if (!recv_recovery_on) {
			mutex_exit(&recv_sys->writer_mutex);
			break;
		}

		/* Flush pages from end of LRU if required */
		os_event_reset(recv_sys->flush_end);
		recv_sys->flush_type = BUF_FLUSH_LRU;
		os_event_set(recv_sys->flush_start);
		os_event_wait(recv_sys->flush_end);

		mutex_exit(&recv_sys->writer_mutex);
	}

	recv_writer_thread_active = false;

	my_thread_end();
	/* We count the number of threads in os_thread_exit().
	A created thread should always use that to exit and not
	use return() to exit. */
	os_thread_exit();

	OS_THREAD_DUMMY_RETURN;
}
#endif /* !UNIV_HOTBACKUP */

/************************************************************
Inits the recovery system for a recovery operation. */
void
recv_sys_init(
/*==========*/
	ulint	available_memory)	/*!< in: available memory in bytes */
{
	if (recv_sys->heap != NULL) {

		return;
	}

#ifndef UNIV_HOTBACKUP
	mutex_enter(&(recv_sys->mutex));

	recv_sys->heap = mem_heap_create_typed(256,
					MEM_HEAP_FOR_RECV_SYS);

	if (!srv_read_only_mode) {
		recv_sys->flush_start = os_event_create(0);
		recv_sys->flush_end = os_event_create(0);
	}
#else /* !UNIV_HOTBACKUP */
	recv_sys->heap = mem_heap_create(256);
	recv_is_from_backup = true;
#endif /* !UNIV_HOTBACKUP */

	/* Set appropriate value of recv_n_pool_free_frames. */
	if (buf_pool_get_curr_size() >= (10 * 1024 * 1024)) {
		/* Buffer pool of size greater than 10 MB. */
		recv_n_pool_free_frames = 512;
	}

	recv_sys->buf = static_cast<byte*>(
		ut_malloc_nokey(RECV_PARSING_BUF_SIZE));
	recv_sys->len = 0;
	recv_sys->recovered_offset = 0;
#if defined (UNIV_PMEMOBJ_PART_PL)
	if (!gb_pmw->ppl->is_new){
		//just create dummy addr_hash
		recv_sys->addr_hash = hash_create(128);
	}
	else {
		recv_sys->addr_hash = hash_create(available_memory / 512);
	}
#else
	recv_sys->addr_hash = hash_create(available_memory / 512);
#endif
	recv_sys->n_addrs = 0;

	recv_sys->apply_log_recs = FALSE;
	recv_sys->apply_batch_on = FALSE;

	recv_sys->last_block_buf_start = static_cast<byte*>(
		ut_malloc_nokey(2 * OS_FILE_LOG_BLOCK_SIZE));

	recv_sys->last_block = static_cast<byte*>(ut_align(
		recv_sys->last_block_buf_start, OS_FILE_LOG_BLOCK_SIZE));

	recv_sys->found_corrupt_log = false;
	recv_sys->found_corrupt_fs = false;
	recv_sys->mlog_checkpoint_lsn = 0;

	recv_max_page_lsn = 0;

	/* Call the constructor for recv_sys_t::dblwr member */
	new (&recv_sys->dblwr) recv_dblwr_t();

	recv_sys->encryption_list = NULL;
	mutex_exit(&(recv_sys->mutex));
}

/********************************************************//**
Empties the hash table when it has been fully processed. */
static
void
recv_sys_empty_hash(void)
/*=====================*/
{
	ut_ad(mutex_own(&(recv_sys->mutex)));

	if (recv_sys->n_addrs != 0) {
		ib::fatal() << recv_sys->n_addrs << " pages with log records"
			" were left unprocessed!";
	}

	hash_table_free(recv_sys->addr_hash);
	mem_heap_empty(recv_sys->heap);

	recv_sys->addr_hash = hash_create(buf_pool_get_curr_size() / 512);
}

#ifndef UNIV_HOTBACKUP

/********************************************************//**
Frees the recovery system. */
void
recv_sys_debug_free(void)
/*=====================*/
{
	mutex_enter(&(recv_sys->mutex));

	hash_table_free(recv_sys->addr_hash);
	mem_heap_free(recv_sys->heap);
	ut_free(recv_sys->buf);
	ut_free(recv_sys->last_block_buf_start);

	recv_sys->buf = NULL;
	recv_sys->heap = NULL;
	recv_sys->addr_hash = NULL;
	recv_sys->last_block_buf_start = NULL;

	/* wake page cleaner up to progress */
	if (!srv_read_only_mode) {
		ut_ad(!recv_recovery_on);
		ut_ad(!recv_writer_thread_active);
		os_event_reset(buf_flush_event);
		os_event_set(recv_sys->flush_start);
	}

	if (recv_sys->encryption_list != NULL) {
		encryption_list_t::iterator	it;

		for (it = recv_sys->encryption_list->begin();
		     it != recv_sys->encryption_list->end();
		     it++) {
			if (it->key != NULL) {
				ut_free(it->key);
				it->key = NULL;
			}
			if (it->iv != NULL) {
				ut_free(it->iv);
				it->iv = NULL;
			}
		}

		recv_sys->encryption_list->swap(*recv_sys->encryption_list);

		UT_DELETE(recv_sys->encryption_list);
		recv_sys->encryption_list = NULL;
	}

	mutex_exit(&(recv_sys->mutex));
}

/********************************************************//**
Copies a log segment from the most up-to-date log group to the other log
groups, so that they all contain the latest log data. Also writes the info
about the latest checkpoint to the groups, and inits the fields in the group
memory structs to up-to-date values. */
static
void
recv_synchronize_groups(void)
/*=========================*/
{
	lsn_t		start_lsn;
	lsn_t		end_lsn;
	lsn_t		recovered_lsn;

	recovered_lsn = recv_sys->recovered_lsn;

	/* Read the last recovered log block to the recovery system buffer:
	the block is always incomplete */

	start_lsn = ut_uint64_align_down(recovered_lsn,
					 OS_FILE_LOG_BLOCK_SIZE);
	end_lsn = ut_uint64_align_up(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE);

	ut_a(start_lsn != end_lsn);

	log_group_read_log_seg(recv_sys->last_block,
			       UT_LIST_GET_FIRST(log_sys->log_groups),
			       start_lsn, end_lsn);

	for (log_group_t* group = UT_LIST_GET_FIRST(log_sys->log_groups);
	     group;
	     group = UT_LIST_GET_NEXT(log_groups, group)) {
		/* Update the fields in the group struct to correspond to
		recovered_lsn */

		log_group_set_fields(group, recovered_lsn);
	}

	/* Copy the checkpoint info to the log; remember that we have
	incremented checkpoint_no by one, and the info will not be written
	over the max checkpoint info, thus making the preservation of max
	checkpoint info on disk certain */

	log_write_checkpoint_info(true);
	log_mutex_enter();
}
#endif /* !UNIV_HOTBACKUP */

/** Check the consistency of a log header block.
@param[in]	log header block
@return true if ok */
static
bool
recv_check_log_header_checksum(
	const byte*	buf)
{
	return(log_block_get_checksum(buf)
	       == log_block_calc_checksum_crc32(buf));
}

#ifndef UNIV_HOTBACKUP
/** Find the latest checkpoint in the format-0 log header.
@param[out]	max_group	log group, or NULL
@param[out]	max_field	LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
@return error code or DB_SUCCESS */
static MY_ATTRIBUTE((warn_unused_result))
dberr_t
recv_find_max_checkpoint_0(
	log_group_t**	max_group,
	ulint*		max_field)
{
	log_group_t*	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	ib_uint64_t	max_no = 0;
	ib_uint64_t	checkpoint_no;
	byte*		buf	= log_sys->checkpoint_buf;

	ut_ad(group->format == 0);
	ut_ad(UT_LIST_GET_NEXT(log_groups, group) == NULL);

	/** Offset of the first checkpoint checksum */
	static const uint CHECKSUM_1 = 288;
	/** Offset of the second checkpoint checksum */
	static const uint CHECKSUM_2 = CHECKSUM_1 + 4;
	/** Most significant bits of the checkpoint offset */
	static const uint OFFSET_HIGH32 = CHECKSUM_2 + 12;
	/** Least significant bits of the checkpoint offset */
	static const uint OFFSET_LOW32 = 16;

	for (ulint field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2;
	     field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1) {
		log_group_header_read(group, field);

		if (static_cast<uint32_t>(ut_fold_binary(buf, CHECKSUM_1))
		    != mach_read_from_4(buf + CHECKSUM_1)
		    || static_cast<uint32_t>(
			    ut_fold_binary(buf + LOG_CHECKPOINT_LSN,
					   CHECKSUM_2 - LOG_CHECKPOINT_LSN))
		    != mach_read_from_4(buf + CHECKSUM_2)) {
			DBUG_PRINT("ib_log",
				   ("invalid pre-5.7.9 checkpoint " ULINTPF,
				    field));
			continue;
		}

		group->state = LOG_GROUP_OK;

		group->lsn = mach_read_from_8(
			buf + LOG_CHECKPOINT_LSN);
		group->lsn_offset = static_cast<ib_uint64_t>(
			mach_read_from_4(buf + OFFSET_HIGH32)) << 32
			| mach_read_from_4(buf + OFFSET_LOW32);
		checkpoint_no = mach_read_from_8(
			buf + LOG_CHECKPOINT_NO);

		DBUG_PRINT("ib_log",
			   ("checkpoint " UINT64PF " at " LSN_PF
			    " found in group " ULINTPF,
			    checkpoint_no, group->lsn, group->id));

		if (checkpoint_no >= max_no) {
			*max_group = group;
			*max_field = field;
			max_no = checkpoint_no;
		}
	}

	if (*max_group != NULL) {
		return(DB_SUCCESS);
	}

	ib::error() << "Upgrade after a crash is not supported."
		" This redo log was created before MySQL 5.7.9,"
		" and we did not find a valid checkpoint."
		" Please follow the instructions at"
		" " REFMAN "upgrading.html";
	return(DB_ERROR);
}

/** Determine if a pre-5.7.9 redo log is clean.
@param[in]	lsn	checkpoint LSN
@return error code
@retval	DB_SUCCESS	if the redo log is clean
@retval DB_ERROR	if the redo log is corrupted or dirty */
static
dberr_t
recv_log_format_0_recover(lsn_t lsn)
{
	log_mutex_enter();
	log_group_t*	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	const lsn_t	source_offset
		= log_group_calc_lsn_offset(lsn, group);
	log_mutex_exit();
	const ulint	page_no
		= (ulint) (source_offset / univ_page_size.physical());
	byte*		buf = log_sys->buf;

	static const char* NO_UPGRADE_RECOVERY_MSG =
		"Upgrade after a crash is not supported."
		" This redo log was created before MySQL 5.7.9";
	static const char* NO_UPGRADE_RTFM_MSG =
		". Please follow the instructions at "
		REFMAN "upgrading.html";

	fil_io(IORequestLogRead, true,
	       page_id_t(group->space_id, page_no),
	       univ_page_size,
	       (ulint) ((source_offset & ~(OS_FILE_LOG_BLOCK_SIZE - 1))
			% univ_page_size.physical()),
	       OS_FILE_LOG_BLOCK_SIZE, buf, NULL);

	if (log_block_calc_checksum_format_0(buf)
	    != log_block_get_checksum(buf)) {
		ib::error() << NO_UPGRADE_RECOVERY_MSG
			<< ", and it appears corrupted"
			<< NO_UPGRADE_RTFM_MSG;
		return(DB_CORRUPTION);
	}

	if (log_block_get_data_len(buf)
	    != (source_offset & (OS_FILE_LOG_BLOCK_SIZE - 1))) {
		ib::error() << NO_UPGRADE_RECOVERY_MSG
			<< NO_UPGRADE_RTFM_MSG;
		return(DB_ERROR);
	}

	/* Mark the redo log for upgrading. */
	srv_log_file_size = 0;
	recv_sys->parse_start_lsn = recv_sys->recovered_lsn
		= recv_sys->scanned_lsn
		= recv_sys->mlog_checkpoint_lsn = lsn;
	log_sys->last_checkpoint_lsn = log_sys->next_checkpoint_lsn
		= log_sys->lsn = log_sys->write_lsn
		= log_sys->current_flush_lsn = log_sys->flushed_to_disk_lsn
		= lsn;
	log_sys->next_checkpoint_no = 0;
	return(DB_SUCCESS);
}

/** Find the latest checkpoint in the log header.
@param[out]	max_group	log group, or NULL
@param[out]	max_field	LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
@return error code or DB_SUCCESS */
static MY_ATTRIBUTE((warn_unused_result))
dberr_t
recv_find_max_checkpoint(
	log_group_t**	max_group,
	ulint*		max_field)
{
	log_group_t*	group;
	ib_uint64_t	max_no;
	ib_uint64_t	checkpoint_no;
	ulint		field;
	byte*		buf;

	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	max_no = 0;
	*max_group = NULL;
	*max_field = 0;

	buf = log_sys->checkpoint_buf;

	while (group) {
		group->state = LOG_GROUP_CORRUPTED;

		log_group_header_read(group, 0);
		/* Check the header page checksum. There was no
		checksum in the first redo log format (version 0). */
		group->format = mach_read_from_4(buf + LOG_HEADER_FORMAT);
		if (group->format != 0
		    && !recv_check_log_header_checksum(buf)) {
			ib::error() << "Invalid redo log header checksum.";
			return(DB_CORRUPTION);
		}

		switch (group->format) {
		case 0:
			return(recv_find_max_checkpoint_0(
				       max_group, max_field));
		case LOG_HEADER_FORMAT_CURRENT:
			break;
		default:
			/* Ensure that the string is NUL-terminated. */
			buf[LOG_HEADER_CREATOR_END] = 0;
			ib::error() << "Unsupported redo log format."
				" The redo log was created"
				" with " << buf + LOG_HEADER_CREATOR <<
				". Please follow the instructions at "
				REFMAN "upgrading-downgrading.html";
			/* Do not issue a message about a possibility
			to cleanly shut down the newer server version
			and to remove the redo logs, because the
			format of the system data structures may
			radically change after MySQL 5.7. */
			return(DB_ERROR);
		}

		for (field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2;
		     field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1) {

			log_group_header_read(group, field);

			if (!recv_check_log_header_checksum(buf)) {
				DBUG_PRINT("ib_log",
					   ("invalid checkpoint,"
					    " group " ULINTPF " at " ULINTPF
					    ", checksum %x",
					    group->id, field,
					    (unsigned) log_block_get_checksum(
						    buf)));
				continue;
			}

			group->state = LOG_GROUP_OK;

			group->lsn = mach_read_from_8(
				buf + LOG_CHECKPOINT_LSN);
			group->lsn_offset = mach_read_from_8(
				buf + LOG_CHECKPOINT_OFFSET);
			checkpoint_no = mach_read_from_8(
				buf + LOG_CHECKPOINT_NO);

			DBUG_PRINT("ib_log",
				   ("checkpoint " UINT64PF " at " LSN_PF
				    " found in group " ULINTPF,
				    checkpoint_no, group->lsn, group->id));

			if (checkpoint_no >= max_no) {
				*max_group = group;
				*max_field = field;
				max_no = checkpoint_no;
			}
		}

		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	if (*max_group == NULL) {
		/* Before 5.7.9, we could get here during database
		initialization if we created an ib_logfile0 file that
		was filled with zeroes, and were killed. After
		5.7.9, we would reject such a file already earlier,
		when checking the file header. */
		ib::error() << "No valid checkpoint found"
			" (corrupted redo log)."
			" You can try --innodb-force-recovery=6"
			" as a last resort.";
		return(DB_ERROR);
	}

	return(DB_SUCCESS);
}
#else /* !UNIV_HOTBACKUP */
/*******************************************************************//**
Reads the checkpoint info needed in hot backup.
@return TRUE if success */
ibool
recv_read_checkpoint_info_for_backup(
/*=================================*/
	const byte*	hdr,	/*!< in: buffer containing the log group
				header */
	lsn_t*		lsn,	/*!< out: checkpoint lsn */
	lsn_t*		offset,	/*!< out: checkpoint offset in the log group */
	lsn_t*		cp_no,	/*!< out: checkpoint number */
	lsn_t*		first_header_lsn)
				/*!< out: lsn of of the start of the
				first log file */
{
	ulint		max_cp		= 0;
	ib_uint64_t	max_cp_no	= 0;
	const byte*	cp_buf;

	cp_buf = hdr + LOG_CHECKPOINT_1;

	if (recv_check_log_header_checksum(cp_buf)) {
		max_cp_no = mach_read_from_8(cp_buf + LOG_CHECKPOINT_NO);
		max_cp = LOG_CHECKPOINT_1;
	}

	cp_buf = hdr + LOG_CHECKPOINT_2;

	if (recv_check_log_header_checksum(cp_buf)) {
		if (mach_read_from_8(cp_buf + LOG_CHECKPOINT_NO) > max_cp_no) {
			max_cp = LOG_CHECKPOINT_2;
		}
	}

	if (max_cp == 0) {
		return(FALSE);
	}

	cp_buf = hdr + max_cp;

	*lsn = mach_read_from_8(cp_buf + LOG_CHECKPOINT_LSN);
	*offset = mach_read_from_8(
		cp_buf + LOG_CHECKPOINT_OFFSET);

	*cp_no = mach_read_from_8(cp_buf + LOG_CHECKPOINT_NO);

	*first_header_lsn = mach_read_from_8(hdr + LOG_HEADER_START_LSN);

	return(TRUE);
}
#endif /* !UNIV_HOTBACKUP */

/** Check the 4-byte checksum to the trailer checksum field of a log
block.
@param[in]	log block
@return whether the checksum matches */
static
bool
log_block_checksum_is_ok(
	const byte*	block)	/*!< in: pointer to a log block */
{
	return(!innodb_log_checksums
	       || log_block_get_checksum(block)
	       == log_block_calc_checksum(block));
}

#ifdef UNIV_HOTBACKUP
/*******************************************************************//**
Scans the log segment and n_bytes_scanned is set to the length of valid
log scanned. */
void
recv_scan_log_seg_for_backup(
/*=========================*/
	byte*		buf,		/*!< in: buffer containing log data */
	ulint		buf_len,	/*!< in: data length in that buffer */
	lsn_t*		scanned_lsn,	/*!< in/out: lsn of buffer start,
					we return scanned lsn */
	ulint*		scanned_checkpoint_no,
					/*!< in/out: 4 lowest bytes of the
					highest scanned checkpoint number so
					far */
	ulint*		n_bytes_scanned)/*!< out: how much we were able to
					scan, smaller than buf_len if log
					data ended here */
{
	ulint	data_len;
	byte*	log_block;
	ulint	no;

	*n_bytes_scanned = 0;

	for (log_block = buf; log_block < buf + buf_len;
	     log_block += OS_FILE_LOG_BLOCK_SIZE) {

		no = log_block_get_hdr_no(log_block);

#if 0
		fprintf(stderr, "Log block header no %lu\n", no);
#endif

		if (no != log_block_convert_lsn_to_no(*scanned_lsn)
		    || !log_block_checksum_is_ok(log_block)) {
#if 0
			fprintf(stderr,
				"Log block n:o %lu, scanned lsn n:o %lu\n",
				no, log_block_convert_lsn_to_no(*scanned_lsn));
#endif
			/* Garbage or an incompletely written log block */

			log_block += OS_FILE_LOG_BLOCK_SIZE;
#if 0
			fprintf(stderr,
				"Next log block n:o %lu\n",
				log_block_get_hdr_no(log_block));
#endif
			break;
		}

		if (*scanned_checkpoint_no > 0
		    && log_block_get_checkpoint_no(log_block)
		    < *scanned_checkpoint_no
		    && *scanned_checkpoint_no
		    - log_block_get_checkpoint_no(log_block)
		    > 0x80000000UL) {

			/* Garbage from a log buffer flush which was made
			before the most recent database recovery */
#if 0
			fprintf(stderr,
				"Scanned cp n:o %lu, block cp n:o %lu\n",
				*scanned_checkpoint_no,
				log_block_get_checkpoint_no(log_block));
#endif
			break;
		}

		data_len = log_block_get_data_len(log_block);

		*scanned_checkpoint_no
			= log_block_get_checkpoint_no(log_block);
		*scanned_lsn += data_len;

		*n_bytes_scanned += data_len;

		if (data_len < OS_FILE_LOG_BLOCK_SIZE) {
			/* Log data ends here */

#if 0
			fprintf(stderr, "Log block data len %lu\n",
				data_len);
#endif
			break;
		}
	}
}
#endif /* UNIV_HOTBACKUP */

/** Parse or process a write encryption info record.
@param[in]	ptr		redo log record
@param[in]	end		end of the redo log buffer
@param[in]	space_id	the tablespace ID
@return log record end, NULL if not a complete record */
static
byte*
fil_write_encryption_parse(
	byte*		ptr,
	const byte*	end,
	ulint		space_id)
{
	fil_space_t*	space;
	ulint		offset;
	ulint		len;
	byte*		key = NULL;
	byte*		iv = NULL;
	bool		is_new = false;

	space = fil_space_get(space_id);
	if (space == NULL) {
		encryption_list_t::iterator	it;

		if (recv_sys->encryption_list == NULL) {
			recv_sys->encryption_list =
				UT_NEW_NOKEY(encryption_list_t());
		}

		for (it = recv_sys->encryption_list->begin();
		     it != recv_sys->encryption_list->end();
		     it++) {
			if (it->space_id == space_id) {
				key = it->key;
				iv = it->iv;
			}
		}

		if (key == NULL) {
			key = static_cast<byte*>(ut_malloc_nokey(
					ENCRYPTION_KEY_LEN));
			iv = static_cast<byte*>(ut_malloc_nokey(
					ENCRYPTION_KEY_LEN));
			is_new = true;
		}
	} else {
		key = space->encryption_key;
		iv = space->encryption_iv;
	}

	offset = mach_read_from_2(ptr);
	ptr += 2;
	len = mach_read_from_2(ptr);

	ptr += 2;
	if (end < ptr + len) {
		return(NULL);
	}

	if (offset >= UNIV_PAGE_SIZE
	    || len + offset > UNIV_PAGE_SIZE
	    || (len != ENCRYPTION_INFO_SIZE_V1
		&& len != ENCRYPTION_INFO_SIZE_V2)) {
		recv_sys->found_corrupt_log = TRUE;
		return(NULL);
	}

#ifdef	UNIV_ENCRYPT_DEBUG
	if (space) {
		fprintf(stderr, "Got %lu from redo log:", space->id);
	}
#endif
	if (!fsp_header_decode_encryption_info(key,
					       iv,
					       ptr)) {
		recv_sys->found_corrupt_log = TRUE;
		ib::warn() << "Encryption information"
			<< " in the redo log of space "
			<< space_id << " is invalid";
	}

	ut_ad(len == ENCRYPTION_INFO_SIZE_V1
	      || len == ENCRYPTION_INFO_SIZE_V2);

	ptr += len;

	if (space == NULL) {
		if (is_new) {
			recv_encryption_t info;

			/* Add key and iv to list */
			info.space_id = space_id;
			info.key = key;
			info.iv = iv;

			recv_sys->encryption_list->push_back(info);
		}
	} else {
		ut_ad(FSP_FLAGS_GET_ENCRYPTION(space->flags));

		space->encryption_type = Encryption::AES;
		space->encryption_klen = ENCRYPTION_KEY_LEN;
	}

	return(ptr);
}

/** Try to parse a single log record body and also applies it if
specified.
@param[in]	type		redo log entry type
@param[in]	ptr		redo log record body
@param[in]	end_ptr		end of buffer
@param[in]	space_id	tablespace identifier
@param[in]	page_no		page number
@param[in,out]	block		buffer block, or NULL if
a page log record should not be applied
or if it is a MLOG_FILE_ operation
@param[in,out]	mtr		mini-transaction, or NULL if
a page log record should not be applied
@return log record end, NULL if not a complete record */
static
byte*
recv_parse_or_apply_log_rec_body(
	mlog_id_t	type,
	byte*		ptr,
	byte*		end_ptr,
	ulint		space_id,
	ulint		page_no,
	buf_block_t*	block,
	mtr_t*		mtr)
{
	ut_ad(!block == !mtr);

	switch (type) {
	case MLOG_FILE_NAME:
	case MLOG_FILE_DELETE:
	case MLOG_FILE_CREATE2:
	case MLOG_FILE_RENAME2:
		ut_ad(block == NULL);
		/* Collect the file names when parsing the log,
		before applying any log records. */
		return(fil_name_parse(ptr, end_ptr, space_id, page_no, type));
	case MLOG_INDEX_LOAD:
#ifdef UNIV_HOTBACKUP
		/* While scaning redo logs during  backup phase a
		MLOG_INDEX_LOAD type redo log record indicates a DDL
		(create index, alter table...)is performed with
		'algorithm=inplace'. This redo log indicates that

		1. The DDL was started after MEB started backing up, in which
		case MEB will not be able to take a consistent backup and should
		fail. or
		2. There is a possibility of this record existing in the REDO
		even after the completion of the index create operation. This is
		because of InnoDB does  not checkpointing after the flushing the
		index pages.

		If MEB gets the last_redo_flush_lsn and that is less than the
		lsn of the current record MEB fails the backup process.
		Error out in case of online backup and emit a warning in case
		of offline backup and continue.
		*/
		if (!recv_recovery_on) {
			if (is_online_redo_copy) {
				if (backup_redo_log_flushed_lsn
				    < recv_sys->recovered_lsn) {
					ib::trace() << "Last flushed lsn: "
						<< backup_redo_log_flushed_lsn
						<< " load_index lsn "
						<< recv_sys->recovered_lsn;

					if (backup_redo_log_flushed_lsn == 0)
						ib::error() << "MEB was not "
							"able to determine the"
							"InnoDB Engine Status";

					ib::fatal() << "An optimized(without"
						" redo logging) DDLoperation"
						" has been performed. All"
						" modified pages may not have"
						" been flushed to the disk yet."
						" \n    MEB will not be able"
						" take a consistent backup."
						" Retry the backup operation";
				}
				/** else the index is flushed to disk before
				backup started hence no error */
			} else {
				/* offline backup */
				ib::trace() << "Last flushed lsn: "
					<< backup_redo_log_flushed_lsn
					<< " load_index lsn "
					<< recv_sys->recovered_lsn;

				ib::warn() << "An optimized(without redo"
					" logging) DDL operation has been"
					" performed. All modified pages may not"
					" have been flushed to the disk yet."
					" \n    This offline backup may not"
					" be consistent";
			}
		}
#endif /* UNIV_HOTBACKUP */
		if (end_ptr < ptr + 8) {
#if defined (UNIV_PMEMOBJ_PART_PL)
			assert(0);
#endif
			return(NULL);
		}
		return(ptr + 8);
	case MLOG_TRUNCATE:
		return(truncate_t::parse_redo_entry(ptr, end_ptr, space_id));
	case MLOG_WRITE_STRING:
		/* For encrypted tablespace, we need to get the
		encryption key information before the page 0 is recovered.
	        Otherwise, redo will not find the key to decrypt
		the data pages. */
		if (page_no == 0 && !is_system_tablespace(space_id)) {
			return(fil_write_encryption_parse(ptr,
							  end_ptr,
							  space_id));
		}
		break;

	default:
		break;
	}

	dict_index_t*	index	= NULL;
	page_t*		page;
	page_zip_des_t*	page_zip;
#ifdef UNIV_DEBUG
	ulint		page_type;
#endif /* UNIV_DEBUG */

	if (block) {
		/* Applying a page log record. */
		page = block->frame;
		page_zip = buf_block_get_page_zip(block);
		ut_d(page_type = fil_page_get_type(page));
	} else {
		/* Parsing a page log record. */
		page = NULL;
		page_zip = NULL;
		ut_d(page_type = FIL_PAGE_TYPE_ALLOCATED);
	}

	const byte*	old_ptr = ptr;

	switch (type) {
#ifdef UNIV_LOG_LSN_DEBUG
	case MLOG_LSN:
		/* The LSN is checked in recv_parse_log_rec(). */
		break;
#endif /* UNIV_LOG_LSN_DEBUG */
	case MLOG_1BYTE: case MLOG_2BYTES: case MLOG_4BYTES: case MLOG_8BYTES:
#if defined (UNIV_PMEMOBJ_PART_PL)
	goto skip_check; //skip debug check
#endif

#ifdef UNIV_DEBUG
		if (page && page_type == FIL_PAGE_TYPE_ALLOCATED
		    && end_ptr >= ptr + 2) {
			/* It is OK to set FIL_PAGE_TYPE and certain
			list node fields on an empty page.  Any other
			write is not OK. */

			/* NOTE: There may be bogus assertion failures for
			dict_hdr_create(), trx_rseg_header_create(),
			trx_sys_create_doublewrite_buf(), and
			trx_sysf_create().
			These are only called during database creation. */
			ulint	offs = mach_read_from_2(ptr);

			switch (type) {
			default:
				ut_error;
			case MLOG_2BYTES:
				/* Note that this can fail when the
				redo log been written with something
				older than InnoDB Plugin 1.0.4. */
				ut_ad(offs == FIL_PAGE_TYPE
				      || offs == IBUF_TREE_SEG_HEADER
				      + IBUF_HEADER + FSEG_HDR_OFFSET
				      || offs == PAGE_BTR_IBUF_FREE_LIST
				      + PAGE_HEADER + FIL_ADDR_BYTE
				      || offs == PAGE_BTR_IBUF_FREE_LIST
				      + PAGE_HEADER + FIL_ADDR_BYTE
				      + FIL_ADDR_SIZE
				      || offs == PAGE_BTR_SEG_LEAF
				      + PAGE_HEADER + FSEG_HDR_OFFSET
				      || offs == PAGE_BTR_SEG_TOP
				      + PAGE_HEADER + FSEG_HDR_OFFSET
				      || offs == PAGE_BTR_IBUF_FREE_LIST_NODE
				      + PAGE_HEADER + FIL_ADDR_BYTE
				      + 0 /*FLST_PREV*/
				      || offs == PAGE_BTR_IBUF_FREE_LIST_NODE
				      + PAGE_HEADER + FIL_ADDR_BYTE
				      + FIL_ADDR_SIZE /*FLST_NEXT*/);
				break;
			case MLOG_4BYTES:
				/* Note that this can fail when the
				redo log been written with something
				older than InnoDB Plugin 1.0.4. */
				ut_ad(0
				      || offs == IBUF_TREE_SEG_HEADER
				      + IBUF_HEADER + FSEG_HDR_SPACE
				      || offs == IBUF_TREE_SEG_HEADER
				      + IBUF_HEADER + FSEG_HDR_PAGE_NO
				      || offs == PAGE_BTR_IBUF_FREE_LIST
				      + PAGE_HEADER/* flst_init */
				      || offs == PAGE_BTR_IBUF_FREE_LIST
				      + PAGE_HEADER + FIL_ADDR_PAGE
				      || offs == PAGE_BTR_IBUF_FREE_LIST
				      + PAGE_HEADER + FIL_ADDR_PAGE
				      + FIL_ADDR_SIZE
				      || offs == PAGE_BTR_SEG_LEAF
				      + PAGE_HEADER + FSEG_HDR_PAGE_NO
				      || offs == PAGE_BTR_SEG_LEAF
				      + PAGE_HEADER + FSEG_HDR_SPACE
				      || offs == PAGE_BTR_SEG_TOP
				      + PAGE_HEADER + FSEG_HDR_PAGE_NO
				      || offs == PAGE_BTR_SEG_TOP
				      + PAGE_HEADER + FIL_ADDR_PAGE
				      + 0 /*FLST_PREV*/
				      || offs == PAGE_BTR_IBUF_FREE_LIST_NODE
				      + PAGE_HEADER + FIL_ADDR_PAGE
				      + FIL_ADDR_SIZE /*FLST_NEXT*/);
				break;
			}
		}
#endif /* UNIV_DEBUG */

#if defined (UNIV_PMEMOBJ_PART_PL)
skip_check: //skip debug check
#endif
		ptr = mlog_parse_nbytes(type, ptr, end_ptr, page, page_zip);
		if (ptr != NULL && page != NULL
		    && page_no == 0 && type == MLOG_4BYTES) {
			ulint	offs = mach_read_from_2(old_ptr);
			switch (offs) {
				fil_space_t*	space;
				ulint		val;
			default:
				break;
			case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
			case FSP_HEADER_OFFSET + FSP_SIZE:
			case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
			case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
				space = fil_space_get(space_id);
				ut_a(space != NULL);
				val = mach_read_from_4(page + offs);

				switch (offs) {
				case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
					space->flags = val;
					break;
				case FSP_HEADER_OFFSET + FSP_SIZE:
					space->size_in_header = val;
					break;
				case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
					space->free_limit = val;
					break;
				case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
					space->free_len = val;
					ut_ad(val == flst_get_len(
						      page + offs));
					break;
				}
			}
		}
		break;
	case MLOG_REC_INSERT: case MLOG_COMP_REC_INSERT:
		ut_ad(!page || fil_page_type_is_index(page_type));

		if (NULL != (ptr = mlog_parse_index(
				     ptr, end_ptr,
				     type == MLOG_COMP_REC_INSERT,
				     &index))) {
			ut_a(!page
			     || (ibool)!!page_is_comp(page)
			     == dict_table_is_comp(index->table));
			ptr = page_cur_parse_insert_rec(FALSE, ptr, end_ptr,
							block, index, mtr);
		}
		break;
	case MLOG_REC_CLUST_DELETE_MARK: case MLOG_COMP_REC_CLUST_DELETE_MARK:
		ut_ad(!page || fil_page_type_is_index(page_type));

		if (NULL != (ptr = mlog_parse_index(
				     ptr, end_ptr,
				     type == MLOG_COMP_REC_CLUST_DELETE_MARK,
				     &index))) {
			ut_a(!page
			     || (ibool)!!page_is_comp(page)
			     == dict_table_is_comp(index->table));
			ptr = btr_cur_parse_del_mark_set_clust_rec(
				ptr, end_ptr, page, page_zip, index);
		}
		break;
	case MLOG_COMP_REC_SEC_DELETE_MARK:
		ut_ad(!page || fil_page_type_is_index(page_type));
		/* This log record type is obsolete, but we process it for
		backward compatibility with MySQL 5.0.3 and 5.0.4. */
		ut_a(!page || page_is_comp(page));
		ut_a(!page_zip);
		ptr = mlog_parse_index(ptr, end_ptr, TRUE, &index);
		if (!ptr) {
			break;
		}
		/* Fall through */
	case MLOG_REC_SEC_DELETE_MARK:
		ut_ad(!page || fil_page_type_is_index(page_type));
		ptr = btr_cur_parse_del_mark_set_sec_rec(ptr, end_ptr,
							 page, page_zip);
		break;
	case MLOG_REC_UPDATE_IN_PLACE: case MLOG_COMP_REC_UPDATE_IN_PLACE:
		ut_ad(!page || fil_page_type_is_index(page_type));

		if (NULL != (ptr = mlog_parse_index(
				     ptr, end_ptr,
				     type == MLOG_COMP_REC_UPDATE_IN_PLACE,
				     &index))) {
			ut_a(!page
			     || (ibool)!!page_is_comp(page)
			     == dict_table_is_comp(index->table));
			ptr = btr_cur_parse_update_in_place(ptr, end_ptr, page,
							    page_zip, index);
		}
		break;
	case MLOG_LIST_END_DELETE: case MLOG_COMP_LIST_END_DELETE:
	case MLOG_LIST_START_DELETE: case MLOG_COMP_LIST_START_DELETE:
		ut_ad(!page || fil_page_type_is_index(page_type));

		if (NULL != (ptr = mlog_parse_index(
				     ptr, end_ptr,
				     type == MLOG_COMP_LIST_END_DELETE
				     || type == MLOG_COMP_LIST_START_DELETE,
				     &index))) {
			ut_a(!page
			     || (ibool)!!page_is_comp(page)
			     == dict_table_is_comp(index->table));
			ptr = page_parse_delete_rec_list(type, ptr, end_ptr,
							 block, index, mtr);
		}
		break;
	case MLOG_LIST_END_COPY_CREATED: case MLOG_COMP_LIST_END_COPY_CREATED:
		ut_ad(!page || fil_page_type_is_index(page_type));

		if (NULL != (ptr = mlog_parse_index(
				     ptr, end_ptr,
				     type == MLOG_COMP_LIST_END_COPY_CREATED,
				     &index))) {
			ut_a(!page
			     || (ibool)!!page_is_comp(page)
			     == dict_table_is_comp(index->table));
			ptr = page_parse_copy_rec_list_to_created_page(
				ptr, end_ptr, block, index, mtr);
		}
		break;
	case MLOG_PAGE_REORGANIZE:
	case MLOG_COMP_PAGE_REORGANIZE:
	case MLOG_ZIP_PAGE_REORGANIZE:
		ut_ad(!page || fil_page_type_is_index(page_type));

		if (NULL != (ptr = mlog_parse_index(
				     ptr, end_ptr,
				     type != MLOG_PAGE_REORGANIZE,
				     &index))) {
			ut_a(!page
			     || (ibool)!!page_is_comp(page)
			     == dict_table_is_comp(index->table));
			ptr = btr_parse_page_reorganize(
				ptr, end_ptr, index,
				type == MLOG_ZIP_PAGE_REORGANIZE,
				block, mtr);
		}
		break;
	case MLOG_PAGE_CREATE: case MLOG_COMP_PAGE_CREATE:
		/* Allow anything in page_type when creating a page. */
		ut_a(!page_zip);
		page_parse_create(block, type == MLOG_COMP_PAGE_CREATE, false);
		break;
	case MLOG_PAGE_CREATE_RTREE: case MLOG_COMP_PAGE_CREATE_RTREE:
		page_parse_create(block, type == MLOG_COMP_PAGE_CREATE_RTREE,
				  true);
		break;
	case MLOG_UNDO_INSERT:
		ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
		ptr = trx_undo_parse_add_undo_rec(ptr, end_ptr, page);
		break;
	case MLOG_UNDO_ERASE_END:
		ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
		ptr = trx_undo_parse_erase_page_end(ptr, end_ptr, page, mtr);
		break;
	case MLOG_UNDO_INIT:
		/* Allow anything in page_type when creating a page. */
		ptr = trx_undo_parse_page_init(ptr, end_ptr, page, mtr);
		break;
	case MLOG_UNDO_HDR_DISCARD:
		ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
		ptr = trx_undo_parse_discard_latest(ptr, end_ptr, page, mtr);
		break;
	case MLOG_UNDO_HDR_CREATE:
	case MLOG_UNDO_HDR_REUSE:
		ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
		ptr = trx_undo_parse_page_header(type, ptr, end_ptr,
						 page, mtr);
		break;
	case MLOG_REC_MIN_MARK: case MLOG_COMP_REC_MIN_MARK:
		ut_ad(!page || fil_page_type_is_index(page_type));
		/* On a compressed page, MLOG_COMP_REC_MIN_MARK
		will be followed by MLOG_COMP_REC_DELETE
		or MLOG_ZIP_WRITE_HEADER(FIL_PAGE_PREV, FIL_NULL)
		in the same mini-transaction. */
		ut_a(type == MLOG_COMP_REC_MIN_MARK || !page_zip);
		ptr = btr_parse_set_min_rec_mark(
			ptr, end_ptr, type == MLOG_COMP_REC_MIN_MARK,
			page, mtr);
		break;
	case MLOG_REC_DELETE: case MLOG_COMP_REC_DELETE:
		ut_ad(!page || fil_page_type_is_index(page_type));

		if (NULL != (ptr = mlog_parse_index(
				     ptr, end_ptr,
				     type == MLOG_COMP_REC_DELETE,
				     &index))) {
			ut_a(!page
			     || (ibool)!!page_is_comp(page)
			     == dict_table_is_comp(index->table));
			ptr = page_cur_parse_delete_rec(ptr, end_ptr,
							block, index, mtr);
		}
		break;
	case MLOG_IBUF_BITMAP_INIT:
		/* Allow anything in page_type when creating a page. */
		ptr = ibuf_parse_bitmap_init(ptr, end_ptr, block, mtr);
		break;
	case MLOG_INIT_FILE_PAGE:
	case MLOG_INIT_FILE_PAGE2:
		/* Allow anything in page_type when creating a page. */
		ptr = fsp_parse_init_file_page(ptr, end_ptr, block);
		break;
	case MLOG_WRITE_STRING:
		ut_ad(!page || page_type != FIL_PAGE_TYPE_ALLOCATED
		      || page_no == 0);
		ptr = mlog_parse_string(ptr, end_ptr, page, page_zip);
		break;
	case MLOG_ZIP_WRITE_NODE_PTR:
		ut_ad(!page || fil_page_type_is_index(page_type));
		ptr = page_zip_parse_write_node_ptr(ptr, end_ptr,
						    page, page_zip);
		break;
	case MLOG_ZIP_WRITE_BLOB_PTR:
		ut_ad(!page || fil_page_type_is_index(page_type));
		ptr = page_zip_parse_write_blob_ptr(ptr, end_ptr,
						    page, page_zip);
		break;
	case MLOG_ZIP_WRITE_HEADER:
		ut_ad(!page || fil_page_type_is_index(page_type));
		ptr = page_zip_parse_write_header(ptr, end_ptr,
						  page, page_zip);
		break;
	case MLOG_ZIP_PAGE_COMPRESS:
		/* Allow anything in page_type when creating a page. */
		ptr = page_zip_parse_compress(ptr, end_ptr,
					      page, page_zip);
		break;
	case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
		if (NULL != (ptr = mlog_parse_index(
				ptr, end_ptr, TRUE, &index))) {

			ut_a(!page || ((ibool)!!page_is_comp(page)
				== dict_table_is_comp(index->table)));
			ptr = page_zip_parse_compress_no_data(
				ptr, end_ptr, page, page_zip, index);
		}
		break;
	default:
		ptr = NULL;
		recv_sys->found_corrupt_log = true;
	}

	if (index) {
		dict_table_t*	table = index->table;

		dict_mem_index_free(index);
		dict_mem_table_free(table);
	}

	return(ptr);
}

/*********************************************************************//**
Calculates the fold value of a page file address: used in inserting or
searching for a log record in the hash table.
@return folded value */
UNIV_INLINE
ulint
recv_fold(
/*======*/
	ulint	space,	/*!< in: space */
	ulint	page_no)/*!< in: page number */
{
	return(ut_fold_ulint_pair(space, page_no));
}

/*********************************************************************//**
Calculates the hash value of a page file address: used in inserting or
searching for a log record in the hash table.
@return folded value */
UNIV_INLINE
ulint
recv_hash(
/*======*/
	ulint	space,	/*!< in: space */
	ulint	page_no)/*!< in: page number */
{
	return(hash_calc_hash(recv_fold(space, page_no), recv_sys->addr_hash));
}

#if defined (UNIV_PMEMOBJ_PART_PL)
UNIV_INLINE
ulint
pm_ppl_recv_hash(
/*======*/
	PMEM_RECV_LINE* recv_line,
	ulint	space,	/*!< in: space */
	ulint	page_no)/*!< in: page number */
{
	return(hash_calc_hash(recv_fold(space, page_no), recv_line->addr_hash));
}

#endif

/*********************************************************************//**
Gets the hashed file address struct for a page.
@return file address struct, NULL if not found from the hash table */
static
recv_addr_t*
recv_get_fil_addr_struct(
/*=====================*/
	ulint	space,	/*!< in: space id */
	ulint	page_no)/*!< in: page number */
{
	recv_addr_t*	recv_addr;

	for (recv_addr = static_cast<recv_addr_t*>(
			HASH_GET_FIRST(recv_sys->addr_hash,
				       recv_hash(space, page_no)));
	     recv_addr != 0;
	     recv_addr = static_cast<recv_addr_t*>(
		     HASH_GET_NEXT(addr_hash, recv_addr))) {

		if (recv_addr->space == space
		    && recv_addr->page_no == page_no) {

			return(recv_addr);
		}
	}

	return(NULL);
}

/*******************************************************************//**
Adds a new log record to the hash table of log records. */
static
void
recv_add_to_hash_table(
/*===================*/
	mlog_id_t	type,		/*!< in: log record type */
	ulint		space,		/*!< in: space id */
	ulint		page_no,	/*!< in: page number */
	byte*		body,		/*!< in: log record body */
	byte*		rec_end,	/*!< in: log record end */
	lsn_t		start_lsn,	/*!< in: start lsn of the mtr */
	lsn_t		end_lsn)	/*!< in: end lsn of the mtr */
{
	recv_t*		recv;
	ulint		len;
	recv_data_t*	recv_data;
	recv_data_t**	prev_field;
	recv_addr_t*	recv_addr;

	ut_ad(type != MLOG_FILE_DELETE);
	ut_ad(type != MLOG_FILE_CREATE2);
	ut_ad(type != MLOG_FILE_RENAME2);
	ut_ad(type != MLOG_FILE_NAME);
	ut_ad(type != MLOG_DUMMY_RECORD);
	ut_ad(type != MLOG_CHECKPOINT);
	ut_ad(type != MLOG_INDEX_LOAD);
	ut_ad(type != MLOG_TRUNCATE);

	len = rec_end - body;

	recv = static_cast<recv_t*>(
		mem_heap_alloc(recv_sys->heap, sizeof(recv_t)));

	recv->type = type;
	recv->len = rec_end - body;
	recv->start_lsn = start_lsn;
	recv->end_lsn = end_lsn;

	recv_addr = recv_get_fil_addr_struct(space, page_no);

	if (recv_addr == NULL) {

		recv_addr = static_cast<recv_addr_t*>(
			mem_heap_alloc(recv_sys->heap, sizeof(recv_addr_t)));

		recv_addr->space = space;
		recv_addr->page_no = page_no;
		recv_addr->state = RECV_NOT_PROCESSED;

		UT_LIST_INIT(recv_addr->rec_list, &recv_t::rec_list);

		HASH_INSERT(recv_addr_t, addr_hash, recv_sys->addr_hash,
			    recv_fold(space, page_no), recv_addr);
		recv_sys->n_addrs++;
		//tdnguyen test how many UNDO pages
		if (type == MLOG_UNDO_INSERT){
			printf("==> PMEM_RECV: UNDO page added to HT, space %zu page %zu\n", space, page_no);
		}
#if 0
		fprintf(stderr, "Inserting log rec for space %lu, page %lu\n",
			space, page_no);
#endif
	}

	UT_LIST_ADD_LAST(recv_addr->rec_list, recv);

	prev_field = &(recv->data);

	/* Store the log record body in chunks of less than UNIV_PAGE_SIZE:
	recv_sys->heap grows into the buffer pool, and bigger chunks could not
	be allocated */

	while (rec_end > body) {

		len = rec_end - body;

		if (len > RECV_DATA_BLOCK_SIZE) {
			len = RECV_DATA_BLOCK_SIZE;
		}

		recv_data = static_cast<recv_data_t*>(
			mem_heap_alloc(recv_sys->heap,
				       sizeof(recv_data_t) + len));

		*prev_field = recv_data;

		memcpy(recv_data + 1, body, len);

		prev_field = &(recv_data->next);

		body += len;
	}

	*prev_field = NULL;
}

/*********************************************************************//**
Copies the log record body from recv to buf. */
static
void
recv_data_copy_to_buf(
/*==================*/
	byte*	buf,	/*!< in: buffer of length at least recv->len */
	recv_t*	recv)	/*!< in: log record */
{
	recv_data_t*	recv_data;
	ulint		part_len;
	ulint		len;

	len = recv->len;
	recv_data = recv->data;

	while (len > 0) {
		if (len > RECV_DATA_BLOCK_SIZE) {
			part_len = RECV_DATA_BLOCK_SIZE;
		} else {
			part_len = len;
		}

		ut_memcpy(buf, ((byte*) recv_data) + sizeof(recv_data_t),
			  part_len);
		buf += part_len;
		len -= part_len;

		recv_data = recv_data->next;
	}
}

/************************************************************************//**
Applies the hashed log records to the page, if the page lsn is less than the
lsn of a log record. This can be called when a buffer page has just been
read in, or also for a page already in the buffer pool. */
void
recv_recover_page_func(
/*===================*/
#ifndef UNIV_HOTBACKUP
	ibool		just_read_in,
				/*!< in: TRUE if the i/o handler calls
				this for a freshly read page */
#endif /* !UNIV_HOTBACKUP */
	buf_block_t*	block)	/*!< in/out: buffer block */
{

#if defined (UNIV_PMEMOBJ_PART_PL)
	if (!gb_pmw->ppl->is_new){
		buf_page_t* bpage = (buf_page_t*) block;

		PMEM_PAGE_LOG_HASHED_LINE* pline;

		pline = pm_ppl_get_line_from_key(
				gb_pmw->pop, gb_pmw->ppl,
				bpage->id.fold());

		assert(pline != NULL);
		
		if (IS_GLOBAL_HASHTABLE){
			/*the global hashtable approach*/
			return pm_ppl_recv_recover_page_func(gb_pmw->pop, gb_pmw->ppl, NULL, just_read_in, block);
		} else {
			return pm_ppl_recv_recover_page_func(gb_pmw->pop, gb_pmw->ppl, pline, just_read_in, block);
		}
	}
#endif
	page_t*		page;
	page_zip_des_t*	page_zip;
	recv_addr_t*	recv_addr;
	recv_t*		recv;
	byte*		buf;
	lsn_t		start_lsn;
	lsn_t		end_lsn;
	lsn_t		page_lsn;
	lsn_t		page_newest_lsn;
	ibool		modification_to_page;
	mtr_t		mtr;

	mutex_enter(&(recv_sys->mutex));

	if (recv_sys->apply_log_recs == FALSE) {

		/* Log records should not be applied now */

		mutex_exit(&(recv_sys->mutex));

		return;
	}

	recv_addr = recv_get_fil_addr_struct(block->page.id.space(),
					     block->page.id.page_no());

	if ((recv_addr == NULL)
	    || (recv_addr->state == RECV_BEING_PROCESSED)
	    || (recv_addr->state == RECV_PROCESSED)) {
		ut_ad(recv_addr == NULL || recv_needed_recovery);

		mutex_exit(&(recv_sys->mutex));

		return;
	}

#ifndef UNIV_HOTBACKUP
	ut_ad(recv_needed_recovery);

	DBUG_PRINT("ib_log",
		   ("Applying log to page %u:%u",
		    recv_addr->space, recv_addr->page_no));
#endif /* !UNIV_HOTBACKUP */

	recv_addr->state = RECV_BEING_PROCESSED;

	mutex_exit(&(recv_sys->mutex));

	mtr_start(&mtr);
	mtr_set_log_mode(&mtr, MTR_LOG_NONE);

	page = block->frame;
	page_zip = buf_block_get_page_zip(block);

#ifndef UNIV_HOTBACKUP
	if (just_read_in) {
		/* Move the ownership of the x-latch on the page to
		this OS thread, so that we can acquire a second
		x-latch on it.  This is needed for the operations to
		the page to pass the debug checks. */

		rw_lock_x_lock_move_ownership(&block->lock);
	}

	ibool	success = buf_page_get_known_nowait(
		RW_X_LATCH, block, BUF_KEEP_OLD,
		__FILE__, __LINE__, &mtr);
	ut_a(success);

	buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);
#endif /* !UNIV_HOTBACKUP */

	/* Read the newest modification lsn from the page */
	page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);

#ifndef UNIV_HOTBACKUP
	/* It may be that the page has been modified in the buffer
	pool: read the newest modification lsn there */

	page_newest_lsn = buf_page_get_newest_modification(&block->page);

	if (page_newest_lsn) {

		page_lsn = page_newest_lsn;
	}
#else /* !UNIV_HOTBACKUP */
	/* In recovery from a backup we do not really use the buffer pool */
	page_newest_lsn = 0;
#endif /* !UNIV_HOTBACKUP */

	modification_to_page = FALSE;
	start_lsn = end_lsn = 0;

	recv = UT_LIST_GET_FIRST(recv_addr->rec_list);

	while (recv) {
		end_lsn = recv->end_lsn;
#if defined (UNIV_PMEMOBJ_PART_PL)
        //in PPL, we don't use InnoDB's log group
#else //original
		ut_ad(end_lsn
		      <= UT_LIST_GET_FIRST(log_sys->log_groups)->scanned_lsn);
#endif //UNIV_PMEMOBJ_PART_PL
		if (recv->len > RECV_DATA_BLOCK_SIZE) {
			/* We have to copy the record body to a separate
			buffer */

			buf = static_cast<byte*>(ut_malloc_nokey(recv->len));

			recv_data_copy_to_buf(buf, recv);
		} else {
			buf = ((byte*)(recv->data)) + sizeof(recv_data_t);
		}

		if (recv->type == MLOG_INIT_FILE_PAGE) {
			page_lsn = page_newest_lsn;

			memset(FIL_PAGE_LSN + page, 0, 8);
			memset(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM
			       + page, 0, 8);

			if (page_zip) {
				memset(FIL_PAGE_LSN + page_zip->data, 0, 8);
			}
		}

		/* If per-table tablespace was truncated and there exist REDO
		records before truncate that are to be applied as part of
		recovery (checkpoint didn't happen since truncate was done)
		skip such records using lsn check as they may not stand valid
		post truncate.
		LSN at start of truncate is recorded and any redo record
		with LSN less than recorded LSN is skipped.
		Note: We can't skip complete recv_addr as same page may have
		valid REDO records post truncate those needs to be applied. */
		bool	skip_recv = false;
		if (srv_was_tablespace_truncated(fil_space_get(recv_addr->space))) {
			lsn_t	init_lsn =
				truncate_t::get_truncated_tablespace_init_lsn(
				recv_addr->space);
			skip_recv = (recv->start_lsn < init_lsn);
		}

		/* Ignore applying the redo logs for tablespace that is
		truncated. Post recovery there is fixup action that will
		restore the tablespace back to normal state.
		Applying redo at this stage can result in error given that
		redo will have action recorded on page before tablespace
		was re-inited and that would lead to an error while applying
		such action. */
		if (recv->start_lsn >= page_lsn
		    && !srv_is_tablespace_truncated(recv_addr->space)
		    && !skip_recv) {

			lsn_t	end_lsn;

			if (!modification_to_page) {

				modification_to_page = TRUE;
				start_lsn = recv->start_lsn;
			}

			DBUG_PRINT("ib_log",
				   ("apply " LSN_PF ":"
				    " %s len " ULINTPF " page %u:%u",
				    recv->start_lsn,
				    get_mlog_string(recv->type), recv->len,
				    recv_addr->space,
				    recv_addr->page_no));

			recv_parse_or_apply_log_rec_body(
				recv->type, buf, buf + recv->len,
				recv_addr->space, recv_addr->page_no,
				block, &mtr);

			end_lsn = recv->start_lsn + recv->len;
			mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);
			mach_write_to_8(UNIV_PAGE_SIZE
					- FIL_PAGE_END_LSN_OLD_CHKSUM
					+ page, end_lsn);

			if (page_zip) {
				mach_write_to_8(FIL_PAGE_LSN
						+ page_zip->data, end_lsn);
			}
		}

		if (recv->len > RECV_DATA_BLOCK_SIZE) {
			ut_free(buf);
		}

		recv = UT_LIST_GET_NEXT(rec_list, recv);
	}

#ifdef UNIV_ZIP_DEBUG
	if (fil_page_index_page_check(page)) {
		page_zip_des_t*	page_zip = buf_block_get_page_zip(block);

		ut_a(!page_zip
		     || page_zip_validate_low(page_zip, page, NULL, FALSE));
	}
#endif /* UNIV_ZIP_DEBUG */

#ifndef UNIV_HOTBACKUP
	if (modification_to_page) {
		ut_a(block);

		log_flush_order_mutex_enter();
		buf_flush_recv_note_modification(block, start_lsn, end_lsn);
		log_flush_order_mutex_exit();
	}
#else /* !UNIV_HOTBACKUP */
	start_lsn = start_lsn; /* Silence compiler */
#endif /* !UNIV_HOTBACKUP */

	/* Make sure that committing mtr does not change the modification
	lsn values of page */

	mtr.discard_modifications();

	mtr_commit(&mtr);

	mutex_enter(&(recv_sys->mutex));

	if (recv_max_page_lsn < page_lsn) {
		recv_max_page_lsn = page_lsn;
	}

	recv_addr->state = RECV_PROCESSED;

	ut_a(recv_sys->n_addrs);
	recv_sys->n_addrs--;

	mutex_exit(&(recv_sys->mutex));

}

#ifndef UNIV_HOTBACKUP
/** Reads in pages which have hashed log records, from an area around a given
page number.
@param[in]	page_id	page id
@return number of pages found */
static
ulint
recv_read_in_area(
	const page_id_t&	page_id)
{
	recv_addr_t* recv_addr;
	ulint	page_nos[RECV_READ_AHEAD_AREA];
	ulint	low_limit;
	ulint	n;

	low_limit = page_id.page_no()
		- (page_id.page_no() % RECV_READ_AHEAD_AREA);

	n = 0;

	for (ulint page_no = low_limit;
	     page_no < low_limit + RECV_READ_AHEAD_AREA;
	     page_no++) {

		recv_addr = recv_get_fil_addr_struct(page_id.space(), page_no);

		const page_id_t	cur_page_id(page_id.space(), page_no);

		if (recv_addr && !buf_page_peek(cur_page_id)) {

			mutex_enter(&(recv_sys->mutex));

			if (recv_addr->state == RECV_NOT_PROCESSED) {
				recv_addr->state = RECV_BEING_READ;

				page_nos[n] = page_no;

				n++;
			}

			mutex_exit(&(recv_sys->mutex));
		}
	}

	buf_read_recv_pages(FALSE, page_id.space(), page_nos, n);
	/*
	fprintf(stderr, "Recv pages at %lu n %lu\n", page_nos[0], n);
	*/
	return(n);
}

/*******************************************************************//**
Empties the hash table of stored log records, applying them to appropriate
pages. */
void
recv_apply_hashed_log_recs(
/*=======================*/
	ibool	allow_ibuf)	/*!< in: if TRUE, also ibuf operations are
				allowed during the application; if FALSE,
				no ibuf operations are allowed, and after
				the application all file pages are flushed to
				disk and invalidated in buffer pool: this
				alternative means that no new log records
				can be generated during the application;
				the caller must in this case own the log
				mutex */
{
#if defined (UNIV_PMEMOBJ_PART_PL)
	if (!gb_pmw->ppl->is_new){
		return (pm_ppl_recv_apply_hashed_log_recs(gb_pmw->pop, gb_pmw->ppl, allow_ibuf) );
	}
#endif
	recv_addr_t* recv_addr;
	ulint	i;
	ibool	has_printed	= FALSE;
	mtr_t	mtr;
loop:
	mutex_enter(&(recv_sys->mutex));

	if (recv_sys->apply_batch_on) {

		mutex_exit(&(recv_sys->mutex));

		os_thread_sleep(500000);

		goto loop;
	}

	ut_ad(!allow_ibuf == log_mutex_own());

	if (!allow_ibuf) {
		recv_no_ibuf_operations = true;
	}
//tdnguyen test
	ulint cnt1, cnt2;

	cnt1 = cnt2 = 0;
	printf("PMEM_TEST total recv_addrs %zu\n", recv_sys->n_addrs);
	for (i = 0; i < hash_get_n_cells(recv_sys->addr_hash); i++) {
		for (recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_FIRST(recv_sys->addr_hash, i));
		     recv_addr != 0;
		     recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_NEXT(addr_hash, recv_addr))) {
			if (srv_is_tablespace_truncated(recv_addr->space)) {
				continue;
			}
			if (recv_addr->state == RECV_DISCARDED) {
				continue;
			}
			const page_id_t		page_id(recv_addr->space,
							recv_addr->page_no);
			bool			found;
			const page_size_t&	page_size
				= fil_space_get_page_size(recv_addr->space,
							  &found);

			ut_ad(found);


				mutex_exit(&(recv_sys->mutex));

				buf_block_t*	block;
				dberr_t			err;
				byte*			page;
				bool read_ok;

				mtr_start(&mtr);

				if (buf_page_peek(page_id)) {

					block = buf_page_get(
						page_id, page_size,
						RW_X_LATCH, &mtr);

					buf_block_dbg_add_level(
						block, SYNC_NO_ORDER_CHECK);

					printf("PMEM_TEST get page from ht (%zu, %zu) \n", page_id.space(), page_id.page_no());
					cnt1++;
				} else {
					read_ok = buf_read_page(
							page_id, page_size);
					assert(read_ok);
					block = buf_page_get(
							page_id, page_size,
							RW_X_LATCH, &mtr);
					printf("PMEM_TEST read page (%zu, %zu) \n", page_id.space(), page_id.page_no());
					cnt2++;
				}
				assert(block != NULL);

				mutex_enter(&(recv_sys->mutex));

				page = block->frame;

				ulint read_space_id = mach_read_from_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
				ulint read_page_no = mach_read_from_4(page + FIL_PAGE_OFFSET);

				if(read_space_id != block->page.id.space() || read_page_no != block->page.id.page_no()){
					printf("PMEM_ERROR in pm_ppl_recv_check_hashed_line(), input (space %zu, page_no %zu) differ read (space %zu, page_no %zu)\n", block->page.id.space(), block->page.id.page_no(), read_space_id, read_page_no);
			}

		}
	}
	printf("PMEM_TEST: cnt1 %zu cnt2 %zu total %zu\n", cnt1, cnt2, recv_sys->n_addrs);
	mutex_exit(&(recv_sys->mutex));
	return;
//end tdnguyen test
	recv_sys->apply_log_recs = TRUE;
	recv_sys->apply_batch_on = TRUE;

	for (i = 0; i < hash_get_n_cells(recv_sys->addr_hash); i++) {

		for (recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_FIRST(recv_sys->addr_hash, i));
		     recv_addr != 0;
		     recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_NEXT(addr_hash, recv_addr))) {

			if (srv_is_tablespace_truncated(recv_addr->space)) {
				/* Avoid applying REDO log for the tablespace
				that is schedule for TRUNCATE. */
				ut_a(recv_sys->n_addrs);
				recv_addr->state = RECV_DISCARDED;
				recv_sys->n_addrs--;
				continue;
			}

			if (recv_addr->state == RECV_DISCARDED) {
				ut_a(recv_sys->n_addrs);
				recv_sys->n_addrs--;
				continue;
			}

			const page_id_t		page_id(recv_addr->space,
							recv_addr->page_no);
			bool			found;
			const page_size_t&	page_size
				= fil_space_get_page_size(recv_addr->space,
							  &found);

			ut_ad(found);

			if (recv_addr->state == RECV_NOT_PROCESSED) {
				if (!has_printed) {
					ib::info() << "Starting an apply batch"
						" of log records"
						" to the database...";
					fputs("InnoDB: Progress in percent: ",
					      stderr);
					has_printed = TRUE;
				}

				mutex_exit(&(recv_sys->mutex));

				if (buf_page_peek(page_id)) {
					buf_block_t*	block;

					mtr_start(&mtr);

					block = buf_page_get(
						page_id, page_size,
						RW_X_LATCH, &mtr);

					buf_block_dbg_add_level(
						block, SYNC_NO_ORDER_CHECK);

					recv_recover_page(FALSE, block);
					mtr_commit(&mtr);
				} else {
					recv_read_in_area(page_id);
				}

				mutex_enter(&(recv_sys->mutex));
			}
		}

		if (has_printed
		    && (i * 100) / hash_get_n_cells(recv_sys->addr_hash)
		    != ((i + 1) * 100)
		    / hash_get_n_cells(recv_sys->addr_hash)) {

			fprintf(stderr, "%lu ", (ulong)
				((i * 100)
				 / hash_get_n_cells(recv_sys->addr_hash)));
		}
	}

	/* Wait until all the pages have been processed */

	while (recv_sys->n_addrs != 0) {

		mutex_exit(&(recv_sys->mutex));

		os_thread_sleep(500000);

		mutex_enter(&(recv_sys->mutex));
	}

	if (has_printed) {

		fprintf(stderr, "\n");
	}

	if (!allow_ibuf) {

		/* Flush all the file pages to disk and invalidate them in
		the buffer pool */

		ut_d(recv_no_log_write = true);
		mutex_exit(&(recv_sys->mutex));
		log_mutex_exit();

		/* Stop the recv_writer thread from issuing any LRU
		flush batches. */
		mutex_enter(&recv_sys->writer_mutex);

		/* Wait for any currently run batch to end. */
		buf_flush_wait_LRU_batch_end();

		os_event_reset(recv_sys->flush_end);
		recv_sys->flush_type = BUF_FLUSH_LIST;
		os_event_set(recv_sys->flush_start);
		os_event_wait(recv_sys->flush_end);

		buf_pool_invalidate();

		/* Allow batches from recv_writer thread. */
		mutex_exit(&recv_sys->writer_mutex);

		log_mutex_enter();
		mutex_enter(&(recv_sys->mutex));
		ut_d(recv_no_log_write = false);

		recv_no_ibuf_operations = false;
	}

	recv_sys->apply_log_recs = FALSE;
	recv_sys->apply_batch_on = FALSE;

	recv_sys_empty_hash();

	if (has_printed) {
		ib::info() << "Apply batch completed";
	}

	mutex_exit(&(recv_sys->mutex));
}
#else /* !UNIV_HOTBACKUP */
/*******************************************************************//**
Applies log records in the hash table to a backup. */
void
recv_apply_log_recs_for_backup(void)
/*================================*/
{
	recv_addr_t*	recv_addr;
	ulint		n_hash_cells;
	buf_block_t*	block;
	bool		success;
	ulint		error;
	ulint		i;
	fil_space_t*	space = NULL;
	page_id_t	page_id;
	recv_sys->apply_log_recs = TRUE;
	recv_sys->apply_batch_on = TRUE;

	block = back_block1;

	ib::info() << "Starting an apply batch of log records to the"
		" database...\n";

	fputs("InnoDB: Progress in percent: ", stderr);

	n_hash_cells = hash_get_n_cells(recv_sys->addr_hash);

	for (i = 0; i < n_hash_cells; i++) {
		/* The address hash table is externally chained */
		recv_addr = static_cast<recv_addr_t*>(hash_get_nth_cell(
					recv_sys->addr_hash, i)->node);

		while (recv_addr != NULL) {

			ib::trace() << "recv_addr {State: " << recv_addr->state
				<< ", Space id: " << recv_addr->space
				<< "Page no: " << recv_addr->page_no
				<< ". index i: " << i << "\n";

			bool			found;
			const page_size_t&	page_size
				= fil_space_get_page_size(recv_addr->space,
							  &found);

			if (!found) {
#if 0
				fprintf(stderr,
					"InnoDB: Warning: cannot apply"
					" log record to"
					" tablespace %lu page %lu,\n"
					"InnoDB: because tablespace with"
					" that id does not exist.\n",
					recv_addr->space, recv_addr->page_no);
#endif
				recv_addr->state = RECV_DISCARDED;

				ut_a(recv_sys->n_addrs);
				recv_sys->n_addrs--;

				goto skip_this_recv_addr;
			}

			/* We simulate a page read made by the buffer pool, to
			make sure the recovery apparatus works ok. We must init
			the block. */

			buf_page_init_for_backup_restore(
				page_id_t(recv_addr->space, recv_addr->page_no),
				page_size, block);

			/* Extend the tablespace's last file if the page_no
			does not fall inside its bounds; we assume the last
			file is auto-extending, and mysqlbackup copied the file
			when it still was smaller */
			fil_space_t*	space
				= fil_space_get(recv_addr->space);

			success = fil_space_extend(
				space, recv_addr->page_no + 1);
			if (!success) {
				ib::fatal() << "Cannot extend tablespace "
					<< recv_addr->space << " to hold "
					<< recv_addr->page_no << " pages";
			}

			/* Read the page from the tablespace file using the
			fil0fil.cc routines */

			const page_id_t	page_id(recv_addr->space,
						recv_addr->page_no);

			if (page_size.is_compressed()) {

				error = fil_io(
					IORequestRead, true,
					page_id,
					page_size, 0, page_size.physical(),
					block->page.zip.data, NULL);

				if (error == DB_SUCCESS
				    && !buf_zip_decompress(block, TRUE)) {
					ut_error;
				}
			} else {

				error = fil_io(
					IORequestRead, true,
					page_id, page_size, 0,
					page_size.logical(),
					block->frame, NULL);
			}

			if (error != DB_SUCCESS) {
				ib::fatal() << "Cannot read from tablespace "
					<< recv_addr->space << " page number "
					<< recv_addr->page_no;
			}

			/* Apply the log records to this page */
			recv_recover_page(FALSE, block);

			/* Write the page back to the tablespace file using the
			fil0fil.cc routines */

			buf_flush_init_for_writing(
				block, block->frame,
				buf_block_get_page_zip(block),
				mach_read_from_8(block->frame + FIL_PAGE_LSN),
				fsp_is_checksum_disabled(
					block->page.id.space()));

			if (page_size.is_compressed()) {

				error = fil_io(
					IORequestWrite, true, page_id,
					page_size, 0, page_size.physical(),
					block->page.zip.data, NULL);
			} else {
				error = fil_io(
					IORequestWrite, true, page_id,
					page_size, 0, page_size.logical(),
					block->frame, NULL);
			}
skip_this_recv_addr:
			recv_addr = static_cast<recv_addr_t*>(HASH_GET_NEXT(
					addr_hash, recv_addr));
		}

		if ((100 * i) / n_hash_cells
		    != (100 * (i + 1)) / n_hash_cells) {
			fprintf(stderr, "%lu ",
				(ulong) ((100 * i) / n_hash_cells));
			fflush(stderr);
		}
	}
	/* write logs in next line */
	fprintf(stderr, "\n");
	recv_sys->apply_log_recs = FALSE;
	recv_sys->apply_batch_on = FALSE;
	recv_sys_empty_hash();
}
#endif /* !UNIV_HOTBACKUP */

/** Tries to parse a single log record.
@param[out]	type		log record type
@param[in]	ptr		pointer to a buffer
@param[in]	end_ptr		end of the buffer
@param[out]	space_id	tablespace identifier
@param[out]	page_no		page number
@param[in]	apply		whether to apply MLOG_FILE_* records
@param[out]	body		start of log record body
@return length of the record, or 0 if the record was not complete */
static
ulint
recv_parse_log_rec(
	mlog_id_t*	type,
	byte*		ptr,
	byte*		end_ptr,
	ulint*		space,
	ulint*		page_no,
	bool		apply,
	byte**		body)
{
	byte*	new_ptr;

	*body = NULL;

	UNIV_MEM_INVALID(type, sizeof *type);
	UNIV_MEM_INVALID(space, sizeof *space);
	UNIV_MEM_INVALID(page_no, sizeof *page_no);
	UNIV_MEM_INVALID(body, sizeof *body);

	if (ptr == end_ptr) {

		return(0);
	}

	switch (*ptr) {
#ifdef UNIV_LOG_LSN_DEBUG
	case MLOG_LSN | MLOG_SINGLE_REC_FLAG:
	case MLOG_LSN:
		new_ptr = mlog_parse_initial_log_record(
			ptr, end_ptr, type, space, page_no);
		if (new_ptr != NULL) {
			const lsn_t	lsn = static_cast<lsn_t>(
				*space) << 32 | *page_no;
			ut_a(lsn == recv_sys->recovered_lsn);
		}

		*type = MLOG_LSN;
		return(new_ptr - ptr);
#endif /* UNIV_LOG_LSN_DEBUG */
	case MLOG_MULTI_REC_END:
	case MLOG_DUMMY_RECORD:
		*type = static_cast<mlog_id_t>(*ptr);
		return(1);
	case MLOG_CHECKPOINT:
		if (end_ptr < ptr + SIZE_OF_MLOG_CHECKPOINT) {
			return(0);
		}
		*type = static_cast<mlog_id_t>(*ptr);
		return(SIZE_OF_MLOG_CHECKPOINT);
	case MLOG_MULTI_REC_END | MLOG_SINGLE_REC_FLAG:
	case MLOG_DUMMY_RECORD | MLOG_SINGLE_REC_FLAG:
	case MLOG_CHECKPOINT | MLOG_SINGLE_REC_FLAG:
		recv_sys->found_corrupt_log = true;
		return(0);
	}

	new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space,
						page_no);
	*body = new_ptr;

	if (UNIV_UNLIKELY(!new_ptr)) {

		return(0);
	}

	new_ptr = recv_parse_or_apply_log_rec_body(
		*type, new_ptr, end_ptr, *space, *page_no, NULL, NULL);

	if (UNIV_UNLIKELY(new_ptr == NULL)) {

		return(0);
	}

	return(new_ptr - ptr);
}

/*******************************************************//**
Calculates the new value for lsn when more data is added to the log. */
static
lsn_t
recv_calc_lsn_on_data_add(
/*======================*/
	lsn_t		lsn,	/*!< in: old lsn */
	ib_uint64_t	len)	/*!< in: this many bytes of data is
				added, log block headers not included */
{
	ulint		frag_len;
	ib_uint64_t	lsn_len;

	frag_len = (lsn % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_HDR_SIZE;
	ut_ad(frag_len < OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE
	      - LOG_BLOCK_TRL_SIZE);
	lsn_len = len;
	lsn_len += (lsn_len + frag_len)
		/ (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE
		   - LOG_BLOCK_TRL_SIZE)
		* (LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE);

	return(lsn + lsn_len);
}

/** Prints diagnostic info of corrupt log.
@param[in]	ptr	pointer to corrupt log record
@param[in]	type	type of the log record (could be garbage)
@param[in]	space	tablespace ID (could be garbage)
@param[in]	page_no	page number (could be garbage)
@return whether processing should continue */
static
bool
recv_report_corrupt_log(
	const byte*	ptr,
	int		type,
	ulint		space,
	ulint		page_no)
{
	ib::error() <<
		"############### CORRUPT LOG RECORD FOUND ##################";
	ib::info() << "Log record type " << type << ", page " << space << ":"
		<< page_no << ". Log parsing proceeded successfully up to "
		<< recv_sys->recovered_lsn << ". Previous log record type "
		<< recv_previous_parsed_rec_type << ", is multi "
		<< recv_previous_parsed_rec_is_multi << " Recv offset "
		<< (ptr - recv_sys->buf) << ", prev "
		<< recv_previous_parsed_rec_offset;

#if defined (UNIV_PMEMOBJ_PART_PL)
	assert(0);
	return false;
#endif
	ut_ad(ptr <= recv_sys->buf + recv_sys->len);

	const ulint	limit	= 100;
	const ulint	before
		= std::min(recv_previous_parsed_rec_offset, limit);
	const ulint	after
		= std::min(recv_sys->len - (ptr - recv_sys->buf), limit);

	ib::info() << "Hex dump starting " << before << " bytes before and"
		" ending " << after << " bytes after the corrupted record:";

	ut_print_buf(stderr,
		     recv_sys->buf
		     + recv_previous_parsed_rec_offset - before,
		     ptr - recv_sys->buf + before + after
		     - recv_previous_parsed_rec_offset);
	putc('\n', stderr);

#ifndef UNIV_HOTBACKUP
	if (!srv_force_recovery) {
		ib::info() << "Set innodb_force_recovery to ignore this error.";
		return(false);
	}
#endif /* !UNIV_HOTBACKUP */

	ib::warn() << "The log file may have been corrupt and it is possible"
		" that the log scan did not proceed far enough in recovery!"
		" Please run CHECK TABLE on your InnoDB tables to check"
		" that they are ok! If mysqld crashes after this recovery; "
		<< FORCE_RECOVERY_MSG;
	return(true);
}

/** Whether to store redo log records to the hash table */
enum store_t {
	/** Do not store redo log records. */
	STORE_NO,
	/** Store redo log records. */
	STORE_YES,
	/** Store redo log records if the tablespace exists. */
	STORE_IF_EXISTS
};

/** Parse log records from a buffer and optionally store them to a
hash table to wait merging to file pages.
@param[in]	checkpoint_lsn	the LSN of the latest checkpoint
@param[in]	store		whether to store page operations
@return whether MLOG_CHECKPOINT record was seen the first time,
or corruption was noticed */
static MY_ATTRIBUTE((warn_unused_result))
bool
recv_parse_log_recs(
	lsn_t		checkpoint_lsn,
	store_t		store)
{
	byte*		ptr;
	byte*		end_ptr;
	bool		single_rec;
	ulint		len;
	lsn_t		new_recovered_lsn;
	lsn_t		old_lsn;
	mlog_id_t	type;
	ulint		space;
	ulint		page_no;
	byte*		body;

	ut_ad(log_mutex_own());
	ut_ad(recv_sys->parse_start_lsn != 0);
loop:
	ptr = recv_sys->buf + recv_sys->recovered_offset;

	end_ptr = recv_sys->buf + recv_sys->len;

	if (ptr == end_ptr) {

		return(false);
	}

	switch (*ptr) {
	case MLOG_CHECKPOINT:
#ifdef UNIV_LOG_LSN_DEBUG
	case MLOG_LSN:
#endif /* UNIV_LOG_LSN_DEBUG */
	case MLOG_DUMMY_RECORD:
		single_rec = true;
		break;
	default:
		single_rec = !!(*ptr & MLOG_SINGLE_REC_FLAG);
	}

	if (single_rec) {
		/* The mtr did not modify multiple pages */

		old_lsn = recv_sys->recovered_lsn;

		/* Try to parse a log record, fetching its type, space id,
		page no, and a pointer to the body of the log record */

		len = recv_parse_log_rec(&type, ptr, end_ptr, &space,
					 &page_no, true, &body);

		if (len == 0) {
			return(false);
		}

		if (recv_sys->found_corrupt_log) {
			recv_report_corrupt_log(
				ptr, type, space, page_no);
			return(true);
		}

		if (recv_sys->found_corrupt_fs) {
			return(true);
		}

		new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

		if (new_recovered_lsn > recv_sys->scanned_lsn) {
			/* The log record filled a log block, and we require
			that also the next log block should have been scanned
			in */

			return(false);
		}

		recv_previous_parsed_rec_type = type;
		recv_previous_parsed_rec_offset = recv_sys->recovered_offset;
		recv_previous_parsed_rec_is_multi = 0;

		recv_sys->recovered_offset += len;
		recv_sys->recovered_lsn = new_recovered_lsn;

		switch (type) {
			lsn_t	lsn;
		case MLOG_DUMMY_RECORD:
			/* Do nothing */
			break;
		case MLOG_CHECKPOINT:
#if SIZE_OF_MLOG_CHECKPOINT != 1 + 8
# error SIZE_OF_MLOG_CHECKPOINT != 1 + 8
#endif
			lsn = mach_read_from_8(ptr + 1);

			DBUG_PRINT("ib_log",
				   ("MLOG_CHECKPOINT(" LSN_PF ") %s at "
				    LSN_PF,
				    lsn,
				    lsn != checkpoint_lsn ? "ignored"
				    : recv_sys->mlog_checkpoint_lsn
				    ? "reread" : "read",
				    recv_sys->recovered_lsn));

			if (lsn == checkpoint_lsn) {
				if (recv_sys->mlog_checkpoint_lsn) {
					/* At recv_reset_logs() we may
					write a duplicate MLOG_CHECKPOINT
					for the same checkpoint LSN. Thus
					recv_sys->mlog_checkpoint_lsn
					can differ from the current LSN. */
					ut_ad(recv_sys->mlog_checkpoint_lsn
					      <= recv_sys->recovered_lsn);
					break;
				}
				recv_sys->mlog_checkpoint_lsn
					= recv_sys->recovered_lsn;
			}
			break;
		case MLOG_FILE_NAME:
		case MLOG_FILE_DELETE:
		case MLOG_FILE_CREATE2:
		case MLOG_FILE_RENAME2:
		case MLOG_TRUNCATE:
			/* These were already handled by
			recv_parse_log_rec() and
			recv_parse_or_apply_log_rec_body(). */
			break;
#ifdef UNIV_LOG_LSN_DEBUG
		case MLOG_LSN:
			/* Do not add these records to the hash table.
			The page number and space id fields are misused
			for something else. */
			break;
#endif /* UNIV_LOG_LSN_DEBUG */
		default:
			switch (store) {
			case STORE_NO:
				break;
			case STORE_IF_EXISTS:
				if (fil_space_get_flags(space)
				    == ULINT_UNDEFINED) {
					break;
				}
				/* fall through */
			case STORE_YES:
				recv_add_to_hash_table(
					type, space, page_no, body,
					ptr + len, old_lsn,
					recv_sys->recovered_lsn);
			}
			/* fall through */
		case MLOG_INDEX_LOAD:
			DBUG_PRINT("ib_log",
				("scan " LSN_PF ": log rec %s"
				" len " ULINTPF
				" page " ULINTPF ":" ULINTPF,
				old_lsn, get_mlog_string(type),
				len, space, page_no));
		}
	} else {
		/* Check that all the records associated with the single mtr
		are included within the buffer */

		ulint	total_len	= 0;
		ulint	n_recs		= 0;
		bool	only_mlog_file	= true;
		ulint	mlog_rec_len	= 0;

		for (;;) {
			len = recv_parse_log_rec(
				&type, ptr, end_ptr, &space, &page_no,
				false, &body);

			if (len == 0) {
				return(false);
			}

			if (recv_sys->found_corrupt_log
			    || type == MLOG_CHECKPOINT
			    || (*ptr & MLOG_SINGLE_REC_FLAG)) {
				recv_sys->found_corrupt_log = true;
				recv_report_corrupt_log(
					ptr, type, space, page_no);
				return(true);
			}

			if (recv_sys->found_corrupt_fs) {
				return(true);
			}

			recv_previous_parsed_rec_type = type;
			recv_previous_parsed_rec_offset
				= recv_sys->recovered_offset + total_len;
			recv_previous_parsed_rec_is_multi = 1;

			/* MLOG_FILE_NAME redo log records doesn't make changes
			to persistent data. If only MLOG_FILE_NAME redo
			log record exists then reset the parsing buffer pointer
			by changing recovered_lsn and recovered_offset. */
			if (type != MLOG_FILE_NAME && only_mlog_file == true) {
				only_mlog_file = false;
			}

			if (only_mlog_file) {
				new_recovered_lsn = recv_calc_lsn_on_data_add(
					recv_sys->recovered_lsn, len);
				mlog_rec_len += len;
				recv_sys->recovered_offset += len;
				recv_sys->recovered_lsn = new_recovered_lsn;
			}

			total_len += len;
			n_recs++;

			ptr += len;

			if (type == MLOG_MULTI_REC_END) {
				DBUG_PRINT("ib_log",
					   ("scan " LSN_PF
					    ": multi-log end"
					    " total_len " ULINTPF
					    " n=" ULINTPF,
					    recv_sys->recovered_lsn,
					    total_len, n_recs));
				total_len -= mlog_rec_len;
				break;
			}

			DBUG_PRINT("ib_log",
				   ("scan " LSN_PF ": multi-log rec %s"
				    " len " ULINTPF
				    " page " ULINTPF ":" ULINTPF,
				    recv_sys->recovered_lsn,
				    get_mlog_string(type), len, space, page_no));
		}

		new_recovered_lsn = recv_calc_lsn_on_data_add(
			recv_sys->recovered_lsn, total_len);

		if (new_recovered_lsn > recv_sys->scanned_lsn) {
			/* The log record filled a log block, and we require
			that also the next log block should have been scanned
			in */

			return(false);
		}

		/* Add all the records to the hash table */

		ptr = recv_sys->buf + recv_sys->recovered_offset;

		for (;;) {
			old_lsn = recv_sys->recovered_lsn;
			/* This will apply MLOG_FILE_ records. We
			had to skip them in the first scan, because we
			did not know if the mini-transaction was
			completely recovered (until MLOG_MULTI_REC_END). */
			len = recv_parse_log_rec(
				&type, ptr, end_ptr, &space, &page_no,
				true, &body);

			if (recv_sys->found_corrupt_log
			    && !recv_report_corrupt_log(
				    ptr, type, space, page_no)) {
				return(true);
			}

			if (recv_sys->found_corrupt_fs) {
				return(true);
			}

			ut_a(len != 0);
			ut_a(!(*ptr & MLOG_SINGLE_REC_FLAG));

			recv_sys->recovered_offset += len;
			recv_sys->recovered_lsn
				= recv_calc_lsn_on_data_add(old_lsn, len);

			switch (type) {
			case MLOG_MULTI_REC_END:
				/* Found the end mark for the records */
				goto loop;
#ifdef UNIV_LOG_LSN_DEBUG
			case MLOG_LSN:
				/* Do not add these records to the hash table.
				The page number and space id fields are misused
				for something else. */
				break;
#endif /* UNIV_LOG_LSN_DEBUG */
			case MLOG_FILE_NAME:
			case MLOG_FILE_DELETE:
			case MLOG_FILE_CREATE2:
			case MLOG_FILE_RENAME2:
			case MLOG_INDEX_LOAD:
			case MLOG_TRUNCATE:
				/* These were already handled by
				recv_parse_log_rec() and
				recv_parse_or_apply_log_rec_body(). */
				break;
			default:
				switch (store) {
				case STORE_NO:
					break;
				case STORE_IF_EXISTS:
					if (fil_space_get_flags(space)
					    == ULINT_UNDEFINED) {
						break;
					}
					/* fall through */
				case STORE_YES:
					recv_add_to_hash_table(
						type, space, page_no,
						body, ptr + len,
						old_lsn,
						new_recovered_lsn);
				}
			}

			ptr += len;
		}
	}

	goto loop;
}

/*******************************************************//**
Adds data from a new log block to the parsing buffer of recv_sys if
recv_sys->parse_start_lsn is non-zero.
@return true if more data added */
static
bool
recv_sys_add_to_parsing_buf(
/*========================*/
	const byte*	log_block,	/*!< in: log block */
	lsn_t		scanned_lsn)	/*!< in: lsn of how far we were able
					to find data in this log block */
{
	ulint	more_len;
	ulint	data_len;
	ulint	start_offset;
	ulint	end_offset;

	ut_ad(scanned_lsn >= recv_sys->scanned_lsn);

	if (!recv_sys->parse_start_lsn) {
		/* Cannot start parsing yet because no start point for
		it found */

		return(false);
	}

	data_len = log_block_get_data_len(log_block);

	if (recv_sys->parse_start_lsn >= scanned_lsn) {

		return(false);

	} else if (recv_sys->scanned_lsn >= scanned_lsn) {

		return(false);

	} else if (recv_sys->parse_start_lsn > recv_sys->scanned_lsn) {
		more_len = (ulint) (scanned_lsn - recv_sys->parse_start_lsn);
	} else {
		more_len = (ulint) (scanned_lsn - recv_sys->scanned_lsn);
	}

	if (more_len == 0) {

		return(false);
	}

	ut_ad(data_len >= more_len);

	start_offset = data_len - more_len;

	if (start_offset < LOG_BLOCK_HDR_SIZE) {
		start_offset = LOG_BLOCK_HDR_SIZE;
	}

	end_offset = data_len;

	if (end_offset > OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
		end_offset = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
	}

	ut_ad(start_offset <= end_offset);

	if (start_offset < end_offset) {
		ut_memcpy(recv_sys->buf + recv_sys->len,
			  log_block + start_offset, end_offset - start_offset);

		recv_sys->len += end_offset - start_offset;

		ut_a(recv_sys->len <= RECV_PARSING_BUF_SIZE);
	}

	return(true);
}

/*******************************************************//**
Moves the parsing buffer data left to the buffer start. */
static
void
recv_sys_justify_left_parsing_buf(void)
/*===================================*/
{
	ut_memmove(recv_sys->buf, recv_sys->buf + recv_sys->recovered_offset,
		   recv_sys->len - recv_sys->recovered_offset);

	recv_sys->len -= recv_sys->recovered_offset;

	recv_sys->recovered_offset = 0;
}

/*******************************************************//**
Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found.  Unless
UNIV_HOTBACKUP is defined, this function will apply log records
automatically when the hash table becomes full.
@return true if not able to scan any more in this log group */
static
bool
recv_scan_log_recs(
/*===============*/
	ulint		available_memory,/*!< in: we let the hash table of recs
					to grow to this size, at the maximum */
	store_t*	store_to_hash,	/*!< in,out: whether the records should be
					stored to the hash table; this is reset
					if just debug checking is needed, or
					when the available_memory runs out */
	const byte*	buf,		/*!< in: buffer containing a log
					segment or garbage */
	ulint		len,		/*!< in: buffer length */
	lsn_t		checkpoint_lsn,	/*!< in: latest checkpoint LSN */
	lsn_t		start_lsn,	/*!< in: buffer start lsn */
	lsn_t*		contiguous_lsn,	/*!< in/out: it is known that all log
					groups contain contiguous log data up
					to this lsn */
	lsn_t*		group_scanned_lsn)/*!< out: scanning succeeded up to
					this lsn */
{
	const byte*	log_block	= buf;
	ulint		no;
	lsn_t		scanned_lsn	= start_lsn;
	bool		finished	= false;
	ulint		data_len;
	bool		more_data	= false;
	ulint		recv_parsing_buf_size = RECV_PARSING_BUF_SIZE;

	ut_ad(start_lsn % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_ad(len % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_ad(len >= OS_FILE_LOG_BLOCK_SIZE);

	do {
		ut_ad(!finished);
		no = log_block_get_hdr_no(log_block);
		ulint expected_no = log_block_convert_lsn_to_no(scanned_lsn);
		if (no != expected_no) {
			/* Garbage or an incompletely written log block.

			We will not report any error, because this can
			happen when InnoDB was killed while it was
			writing redo log. We simply treat this as an
			abrupt end of the redo log. */
			finished = true;
			break;
		}

		if (!log_block_checksum_is_ok(log_block)) {
			ib::error() << "Log block " << no <<
				" at lsn " << scanned_lsn << " has valid"
				" header, but checksum field contains "
				<< log_block_get_checksum(log_block)
				<< ", should be "
				<< log_block_calc_checksum(log_block);
			/* Garbage or an incompletely written log block.

			This could be the result of killing the server
			while it was writing this log block. We treat
			this as an abrupt end of the redo log. */
			finished = true;
			break;
		}

		if (log_block_get_flush_bit(log_block)) {
			/* This block was a start of a log flush operation:
			we know that the previous flush operation must have
			been completed for all log groups before this block
			can have been flushed to any of the groups. Therefore,
			we know that log data is contiguous up to scanned_lsn
			in all non-corrupt log groups. */

			if (scanned_lsn > *contiguous_lsn) {
				*contiguous_lsn = scanned_lsn;
			}
		}

		data_len = log_block_get_data_len(log_block);

		if (scanned_lsn + data_len > recv_sys->scanned_lsn
		    && log_block_get_checkpoint_no(log_block)
		    < recv_sys->scanned_checkpoint_no
		    && (recv_sys->scanned_checkpoint_no
			- log_block_get_checkpoint_no(log_block)
			> 0x80000000UL)) {

			/* Garbage from a log buffer flush which was made
			before the most recent database recovery */
			finished = true;
			break;
		}

		if (!recv_sys->parse_start_lsn
		    && (log_block_get_first_rec_group(log_block) > 0)) {

			/* We found a point from which to start the parsing
			of log records */

			recv_sys->parse_start_lsn = scanned_lsn
				+ log_block_get_first_rec_group(log_block);
			recv_sys->scanned_lsn = recv_sys->parse_start_lsn;
			recv_sys->recovered_lsn = recv_sys->parse_start_lsn;
		}

		scanned_lsn += data_len;

		if (scanned_lsn > recv_sys->scanned_lsn) {

			/* We have found more entries. If this scan is
			of startup type, we must initiate crash recovery
			environment before parsing these log records. */

#ifndef UNIV_HOTBACKUP
			if (!recv_needed_recovery) {

				if (!srv_read_only_mode) {
					ib::info() << "Log scan progressed"
						" past the checkpoint lsn "
						<< recv_sys->scanned_lsn;

					recv_init_crash_recovery();
				} else {

					ib::warn() << "Recovery skipped,"
						" --innodb-read-only set!";

					return(true);
				}
			}
#endif /* !UNIV_HOTBACKUP */

			/* We were able to find more log data: add it to the
			parsing buffer if parse_start_lsn is already
			non-zero */

			DBUG_EXECUTE_IF(
				"reduce_recv_parsing_buf",
				recv_parsing_buf_size
					= (70 * 1024);
				);

			if (recv_sys->len + 4 * OS_FILE_LOG_BLOCK_SIZE
			    >= recv_parsing_buf_size) {
				ib::error() << "Log parsing buffer overflow."
					" Recovery may have failed!";

				recv_sys->found_corrupt_log = true;

#ifndef UNIV_HOTBACKUP
				if (!srv_force_recovery) {
					ib::error()
						<< "Set innodb_force_recovery"
						" to ignore this error.";
					return(true);
				}
#endif /* !UNIV_HOTBACKUP */

			} else if (!recv_sys->found_corrupt_log) {
				more_data = recv_sys_add_to_parsing_buf(
					log_block, scanned_lsn);
			}

			recv_sys->scanned_lsn = scanned_lsn;
			recv_sys->scanned_checkpoint_no
				= log_block_get_checkpoint_no(log_block);
		}

		if (data_len < OS_FILE_LOG_BLOCK_SIZE) {
			/* Log data for this group ends here */
			finished = true;
			break;
		} else {
			log_block += OS_FILE_LOG_BLOCK_SIZE;
		}
	} while (log_block < buf + len);

	*group_scanned_lsn = scanned_lsn;

	if (recv_needed_recovery
	    || (recv_is_from_backup && !recv_is_making_a_backup)) {
		recv_scan_print_counter++;

		if (finished || (recv_scan_print_counter % 80 == 0)) {

			ib::info() << "Doing recovery: scanned up to"
				" log sequence number " << scanned_lsn;
		}
	}

	if (more_data && !recv_sys->found_corrupt_log) {
		/* Try to parse more log records */

		if (recv_parse_log_recs(checkpoint_lsn,
					*store_to_hash)) {
			ut_ad(recv_sys->found_corrupt_log
			      || recv_sys->found_corrupt_fs
			      || recv_sys->mlog_checkpoint_lsn
			      == recv_sys->recovered_lsn);
			return(true);
		}

		if (*store_to_hash != STORE_NO
		    && mem_heap_get_size(recv_sys->heap) > available_memory) {
			*store_to_hash = STORE_NO;
		}

		if (recv_sys->recovered_offset > recv_parsing_buf_size / 4) {
			/* Move parsing buffer data to the buffer start */

			recv_sys_justify_left_parsing_buf();
		}
	}

	return(finished);
}

#ifndef UNIV_HOTBACKUP
/** Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found.
@param[in,out]	group			log group
@param[in,out]	contiguous_lsn		log sequence number
until which all redo log has been scanned
@param[in]	last_phase		whether changes
can be applied to the tablespaces
@return whether rescan is needed (not everything was stored) */
static
bool
recv_group_scan_log_recs(
	log_group_t*	group,
	lsn_t*		contiguous_lsn,
	bool		last_phase)
{
	DBUG_ENTER("recv_group_scan_log_recs");
	DBUG_ASSERT(!last_phase || recv_sys->mlog_checkpoint_lsn > 0);

	mutex_enter(&recv_sys->mutex);
	recv_sys->len = 0;
	recv_sys->recovered_offset = 0;
	recv_sys->n_addrs = 0;
	recv_sys_empty_hash();
	srv_start_lsn = *contiguous_lsn;
	recv_sys->parse_start_lsn = *contiguous_lsn;
	recv_sys->scanned_lsn = *contiguous_lsn;
	recv_sys->recovered_lsn = *contiguous_lsn;
	recv_sys->scanned_checkpoint_no = 0;
	recv_previous_parsed_rec_type = MLOG_SINGLE_REC_FLAG;
	recv_previous_parsed_rec_offset	= 0;
	recv_previous_parsed_rec_is_multi = 0;
	ut_ad(recv_max_page_lsn == 0);
	ut_ad(last_phase || !recv_writer_thread_active);
	mutex_exit(&recv_sys->mutex);

	lsn_t	checkpoint_lsn	= *contiguous_lsn;
	lsn_t	start_lsn;
	lsn_t	end_lsn;
	store_t	store_to_hash	= last_phase ? STORE_IF_EXISTS : STORE_YES;
	ulint	available_mem	= UNIV_PAGE_SIZE
		* (buf_pool_get_n_pages()
		   - (recv_n_pool_free_frames * srv_buf_pool_instances));

	end_lsn = *contiguous_lsn = ut_uint64_align_down(
		*contiguous_lsn, OS_FILE_LOG_BLOCK_SIZE);

	do {
		if (last_phase && store_to_hash == STORE_NO) {
			store_to_hash = STORE_IF_EXISTS;
			/* We must not allow change buffer
			merge here, because it would generate
			redo log records before we have
			finished the redo log scan. */
			recv_apply_hashed_log_recs(FALSE);
		}

		start_lsn = end_lsn;
		end_lsn += RECV_SCAN_SIZE;

		log_group_read_log_seg(
			log_sys->buf, group, start_lsn, end_lsn);
	} while (!recv_scan_log_recs(
			 available_mem, &store_to_hash, log_sys->buf,
			 RECV_SCAN_SIZE,
			 checkpoint_lsn,
			 start_lsn, contiguous_lsn, &group->scanned_lsn));

	if (recv_sys->found_corrupt_log || recv_sys->found_corrupt_fs) {
		DBUG_RETURN(false);
	}

	DBUG_PRINT("ib_log", ("%s " LSN_PF
			      " completed for log group " ULINTPF,
			      last_phase ? "rescan" : "scan",
			      group->scanned_lsn, group->id));

	DBUG_RETURN(store_to_hash == STORE_NO);
}

/*******************************************************//**
Initialize crash recovery environment. Can be called iff
recv_needed_recovery == false. */
static
void
recv_init_crash_recovery(void)
{
	ut_ad(!srv_read_only_mode);
	ut_a(!recv_needed_recovery);

	recv_needed_recovery = true;
}

/** Report a missing tablespace for which page-redo log exists.
@param[in]	err	previous error code
@param[in]	i	tablespace descriptor
@return new error code */
static
dberr_t
recv_init_missing_space(dberr_t err, const recv_spaces_t::const_iterator& i)
{
	if (srv_force_recovery == 0) {
		ib::error() << "Tablespace " << i->first << " was not"
			" found at " << i->second.name << ".";

		if (err == DB_SUCCESS) {
			ib::error() << "Set innodb_force_recovery=1 to"
				" ignore this and to permanently lose"
				" all changes to the tablespace.";
			err = DB_TABLESPACE_NOT_FOUND;
		}
	} else {
		ib::warn() << "Tablespace " << i->first << " was not"
			" found at " << i->second.name << ", and"
			" innodb_force_recovery was set. All redo log"
			" for this tablespace will be ignored!";
	}

	return(err);
}

/** Report a missing mlog_file_name or mlog_file_delete record for
the tablespace.
@param[in]	recv_addr	Hashed page file address. */
static
void
recv_init_missing_mlog(
	recv_addr_t*	recv_addr)
{
	ulint	space_id = recv_addr->space;
	ulint	page_no = recv_addr->page_no;
	ulint	type = UT_LIST_GET_FIRST(recv_addr->rec_list)->type;
	ulint	start_lsn = UT_LIST_GET_FIRST(recv_addr->rec_list)->start_lsn;

	ib::fatal() << "Missing MLOG_FILE_NAME or MLOG_FILE_DELETE "
		"for redo log record " << type << " (page "
		<< space_id << ":" << page_no << ") at "
		<< start_lsn;
}

/** Check if all tablespaces were found for crash recovery.
@return error code or DB_SUCCESS */
static MY_ATTRIBUTE((warn_unused_result))
dberr_t
recv_init_crash_recovery_spaces(void)
{
	typedef std::set<ulint>	space_set_t;
	bool		flag_deleted	= false;
	space_set_t	missing_spaces;

	ut_ad(!srv_read_only_mode);
	ut_ad(recv_needed_recovery);

	ib::info() << "Database was not shutdown normally!";
	ib::info() << "Starting crash recovery.";

	for (recv_spaces_t::iterator i = recv_spaces.begin();
	     i != recv_spaces.end(); i++) {
		ut_ad(!is_predefined_tablespace(i->first));

		if (i->second.deleted) {
			/* The tablespace was deleted,
			so we can ignore any redo log for it. */
			flag_deleted = true;
		} else if (i->second.space != NULL) {
			/* The tablespace was found, and there
			are some redo log records for it. */
			fil_names_dirty(i->second.space);
		} else {
			missing_spaces.insert(i->first);
			flag_deleted = true;
		}
	}

	if (flag_deleted) {
		dberr_t err = DB_SUCCESS;

		for (ulint h = 0;
		     h < hash_get_n_cells(recv_sys->addr_hash);
		     h++) {
			for (recv_addr_t* recv_addr
				     = static_cast<recv_addr_t*>(
					     HASH_GET_FIRST(
						     recv_sys->addr_hash, h));
			     recv_addr != 0;
			     recv_addr = static_cast<recv_addr_t*>(
				     HASH_GET_NEXT(addr_hash, recv_addr))) {
				const ulint space = recv_addr->space;

				if (is_predefined_tablespace(space)) {
					continue;
				}

				recv_spaces_t::iterator i
					= recv_spaces.find(space);
				
				if (i == recv_spaces.end()) {
					recv_init_missing_mlog(recv_addr);
					recv_addr->state = RECV_DISCARDED;
					continue;
				}

				if (i->second.deleted) {
					ut_ad(missing_spaces.find(space)
					      == missing_spaces.end());
					recv_addr->state = RECV_DISCARDED;
					continue;
				}

				space_set_t::iterator m = missing_spaces.find(
					space);

				if (m != missing_spaces.end()) {
					missing_spaces.erase(m);
					err = recv_init_missing_space(err, i);
					recv_addr->state = RECV_DISCARDED;
					/* All further redo log for this
					tablespace should be removed. */
					i->second.deleted = true;
				}
			}
		}

		if (err != DB_SUCCESS) {
			return(err);
		}
	}

	for (space_set_t::const_iterator m = missing_spaces.begin();
	     m != missing_spaces.end(); m++) {
		recv_spaces_t::iterator i = recv_spaces.find(*m);
		ut_ad(i != recv_spaces.end());

		ib::info() << "Tablespace " << i->first
			<< " was not found at '" << i->second.name
			<< "', but there were no modifications either.";
	}
#if defined (UNIV_PMEMOBJ_BUF)
	//We don't need the torn page correction process, skip this 
#else //original
	buf_dblwr_process();
#endif

	if (srv_force_recovery < SRV_FORCE_NO_LOG_REDO) {
		/* Spawn the background thread to flush dirty pages
		from the buffer pools. */
		os_thread_create(recv_writer_thread, 0, 0);
	}

	return(DB_SUCCESS);
}
#if defined (UNIV_PMEMOBJ_PART_PL)
/*
 * Alternative to recv_sys_init
 * */
void 
pm_ppl_recv_init(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl
		) 
{

	uint32_t n, i;
    uint32_t size;
	ulint avail_mem;
	ulint hash_size;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_RECV_LINE* recv_line;

	n = ppl->n_buckets;
	
	if (IS_GLOBAL_HASHTABLE){
		//A: allocate global recv_line
		avail_mem = buf_pool_get_curr_size();

		ppl->recv_line = (PMEM_RECV_LINE*) malloc(sizeof(PMEM_RECV_LINE));
		recv_line = ppl->recv_line;

		recv_line->heap = mem_heap_create_typed(256,
				MEM_HEAP_FOR_RECV_SYS);

		recv_line-> buf = NULL;
		recv_line->len = 0;
		recv_line->recovered_offset = 0;

		/*Hash table*/
		recv_line->hashed_id = n + 1;	
		//TODO: how to decide the size of the hashtable
		hash_size = avail_mem / 512;
		recv_line->alloc_hash_size = hash_size;
		//hash_size = avail_mem / 4;

		recv_line->addr_hash = hash_create(hash_size);
		printf("PMEM_INFO allocate %zu bytes for ppl->recv_line->addr_hash\n", hash_size);
		recv_line->n_addrs = 0;

		recv_line->apply_log_recs = FALSE;
		recv_line->apply_batch_on = FALSE;

		recv_line->found_corrupt_log = false;
		recv_line->found_corrupt_fs = false;
		recv_line->mlog_checkpoint_lsn = 0;

		recv_line->encryption_list = NULL;
	} else {
		//B: allocate for each line
		for (i = 0; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);

			// the redo buffer has same length as log buffer 
			//size = 2 * D_RW(pline->logbuf)->size;
			size = D_RW(pline->logbuf)->size;

			pline->recv_line = (PMEM_RECV_LINE*) malloc(sizeof(PMEM_RECV_LINE));
			recv_line = pline->recv_line;

			//allocate heap
			recv_line->heap = mem_heap_create_typed(256,
					MEM_HEAP_FOR_RECV_SYS);

			/*the buffer*/
			recv_line-> buf = static_cast<byte*>(
					ut_malloc_nokey(size));
			recv_line->len = size;
			recv_line->recovered_offset = 0;

			/*Hash table*/
			//recv_line->addr_hash = hash_create(avail_mem / 512);
			recv_line->hashed_id = i;	
			//number of cell in a hashtable
			//TODO: how to decide the size of the hashtable
			recv_line->alloc_hash_size = 2048;
			recv_line->addr_hash = hash_create(2048);
			recv_line->n_addrs = 0;

			recv_line->apply_log_recs = FALSE;
			recv_line->apply_batch_on = FALSE;

			recv_line->found_corrupt_log = false;
			recv_line->found_corrupt_fs = false;
			recv_line->mlog_checkpoint_lsn = 0;

			recv_line->encryption_list = NULL;
		} //end for
	}
}

/*
 * Free resource created during recovery process
 * */
void 
pm_ppl_recv_end(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl) {

	uint32_t n, i;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_RECV_LINE* recv_line;

	n = ppl->n_buckets;
	if (IS_GLOBAL_HASHTABLE) {
		//A: free global recv_line
		recv_line = ppl->recv_line;
		recv_line-> buf = NULL;
		recv_line->len = 0;
		recv_line->recovered_offset = 0;
		recv_line->apply_log_recs = FALSE;
		recv_line->apply_batch_on = FALSE;

		if (recv_line->addr_hash != NULL) {
			hash_table_free(recv_line->addr_hash);
		}

		if (recv_line->heap != NULL) {
			mem_heap_free(recv_line->heap);
		}

		//ut_free(recv_line->buf);
		free(recv_line);
		recv_line = NULL;
	} else {
		//B: free other recv_line
		for (i = 0; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);
			recv_line = pline->recv_line;

			if (recv_line != NULL){
				if (recv_line->addr_hash != NULL) {
					hash_table_free(recv_line->addr_hash);
				}

				if (recv_line->heap != NULL) {
					mem_heap_free(recv_line->heap);
				}

				ut_free(recv_line->buf);
				free(recv_line);
				recv_line = NULL;
			}
		}
	}

	//C: reset data structures in PPL
	pm_ppl_reset_all(pop, ppl);
}

/*
 * Simulate recv_recovery_from_checkpoint_start()
 * */
dberr_t
pm_ppl_recovery(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
        lsn_t flush_lsn
        ) 
{

	uint32_t i;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;

	lsn_t		checkpoint_lsn;
	lsn_t		max_recovered_lsn;
	bool		rescan;
	dberr_t		err;
	ulint		avail_mem;
    
	ulint n = ppl->n_buckets;
	//ulint avail_mem = buf_pool_get_curr_size();
	//avail_mem = avail_mem / 512 / n;
	
	//avail_mem is the heap memory for each recv_line	
    //pm_ppl_recv_init(pop, ppl, avail_mem);
    pm_ppl_recv_init(pop, ppl);

	/* Initialize red-black tree for fast insertions into the
	flush_list during recovery process. */
	buf_flush_init_flush_rbt();

	recv_recovery_on = true;
	log_mutex_enter();
    
	/*Phase 1: Analysis and prepare data structure for recovery
     *pm_ppl_analysis() replace old codes until the first recv_group_scan_log_recs()
     * */
    pm_ppl_analysis(pop, ppl);
    printf("\nPMEM_REC: ====    ANALYSIS FINISH ====== \n");
	/*Phase 2: REDO*/
    //Simulate recv_group_scan_log_recs(), we do in parallelism
    pm_ppl_redo(pop, ppl);
	
	/* Check the mem heap after REDO phase 1	*/
	if (IS_GLOBAL_HASHTABLE) {
		ulint cur_heap_size = mem_heap_get_size(ppl->recv_line->heap);
		printf("AFTER PARSE, cur_heap_size %zu vs alloc hash size %zu\n", cur_heap_size, ppl->recv_line->alloc_hash_size);
	} else {
		ulint n = ppl->n_buckets;
		ulint i;
		ulint		total_heap_used = 0;
		
		for (i = 0; i < n; i++){
			pline = D_RW(D_RW(ppl->buckets)[i]);
			recv_line = pline->recv_line;
			ulint cur_heap_size = mem_heap_get_size(recv_line->heap);
			total_heap_used += cur_heap_size;
			ulint n_cells = hash_get_n_cells(recv_line->addr_hash);

			printf("AFTER PARSE, pline %zu cur_heap_size %zu vs alloc hash size %zu n_addrs %zu vs n_cells %zu\n", pline->hashed_id, cur_heap_size, recv_line->alloc_hash_size, recv_line->n_addrs, n_cells);
		}
		printf("AFTER PARSE, total heap used %zu\n", total_heap_used);
		
	}
	max_recovered_lsn = pm_ppl_recv_get_max_recovered_lsn(
		pop, ppl);	
	

    // all code related with checkpoint is removed because we don't use checkpoint
    
    //at the end of pm_ppl_redo, recv_sys->recovered_lsn is the last offset of the last line
	
	log_sys->lsn = max_recovered_lsn;

	recv_needed_recovery = true;


    //this function call buf_dblwr_process() that correct tone page using DWB
	err = pm_ppl_recv_init_crash_recovery_spaces(pop, ppl, max_recovered_lsn);
   

    if (err != DB_SUCCESS) {
        log_mutex_exit();
        return(err);
    }

    
    //we don't need to rescan because we put log recs in HASH table in the first scan


	/* Synchronize the uncorrupted log groups to the most up-to-date log
	group; we also copy checkpoint info to groups */
	
	/*simulate fil_names_clear(log_sys->last_checkpoint_lsn, true)*/
	pm_ppl_fil_names_clear(max_recovered_lsn);

	/*IMPORTANT 
	 * Set apply_log_recs for each recv_line to TRUE, so that UNDO pages and other data pages can be applied (actual REDOing) when read on.
*/	
	
	n = ppl->n_buckets;
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		pline->recv_line->apply_log_recs = TRUE;
	}

	if (IS_GLOBAL_HASHTABLE){
		ppl->recv_line->apply_log_recs = TRUE;	

		/*this function only used in global hashtable approach*/	
		pm_ppl_recv_apply_prior_pages(pop, ppl, NULL, false);
	}

	
	srv_start_lsn = max_recovered_lsn;

	mutex_enter(&recv_sys->mutex);

	recv_sys->apply_log_recs = TRUE;

	mutex_exit(&recv_sys->mutex);

	log_mutex_exit();

	recv_lsn_checks_on = true;
    //pm_ppl_recv_end(pop, ppl);
	
	//printf("PMEM_RECV: RESET PPL after REDO phase 1\n");	
	//pm_page_part_log_bucket_reset(pop, ppl);

	return(DB_SUCCESS);

}

/*
 * Compute the low-water mark recv_diskaddr and corresponding LSN for each lines
 *
 * We still need this step even though  pline->oldest_block_off could tell the same info.
 *
 * pline->oldest_block_off could be out-of-date if the system crashes after the plogblock is reset but before the pm_ppl_update_oldest()
 * */
void 
pm_ppl_analysis(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl)
{

	uint32_t n, i, j;
	int32_t min_id1, min_id2;

    uint64_t low_diskaddr;
    uint64_t low_offset;
    uint64_t low_watermark;
	uint64_t min_lsn;
	
	int64_t delta;
	uint64_t min_delta;
	uint64_t max_delta;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	//k = ppl->n_blocks_per_bucket;
	
	min_delta = ULONG_MAX;
	max_delta = 0;

    //find low_watermark for each line
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

        low_watermark = ULONG_MAX;
        low_diskaddr = ULONG_MAX;
        low_offset = 0;
		min_lsn = ULONG_MAX;
		min_id1 = min_id2 = -1;

        //for each block in the line
        for (j = 0; j < pline->max_blocks; j++){
            plog_block = D_RW(D_RW(pline->arr)[j]);

            if (!plog_block->is_free){
                if(low_watermark > 
                        (plog_block->start_diskaddr + plog_block->start_off)){
				
                    low_watermark = (plog_block->start_diskaddr + plog_block->start_off);
                    low_diskaddr = plog_block->start_diskaddr;
                    low_offset = plog_block->start_off;

					//assert(min_lsn > plog_block->firstLSN);
					min_id1 = j;
                }
            }
        } //end for each block in the line

        for (j = 0; j < pline->max_blocks; j++){
            plog_block = D_RW(D_RW(pline->arr)[j]);

            if (!plog_block->is_free){
				if (min_lsn > plog_block->firstLSN){
					min_lsn = plog_block->firstLSN;
					min_id2 = j;
				}
			}
		}
		//whether min water_mark and min lsn is on the same block?
		if (min_id1 != min_id2){

			PMEM_PAGE_LOG_BLOCK*	plog_block1 = 
				D_RW(D_RW(pline->arr)[min_id1]);

			PMEM_PAGE_LOG_BLOCK*	plog_block2 = 
				D_RW(D_RW(pline->arr)[min_id2]);
			printf("PMEM_WARN: ANALYSIS min lsn is not in same block with low watermark\n");
			assert(min_id1 == min_id2);
		}

        pline->recv_diskaddr = low_diskaddr;
        pline->recv_off = low_offset;
		pline->recv_lsn = min_lsn;

		PMEM_PAGE_LOG_BUF* plogbuf = D_RW(pline->logbuf);
		delta = (pline->diskaddr + plogbuf->cur_off) - (pline->recv_diskaddr + pline->recv_off);
		if (min_delta > delta)
			min_delta = delta;
		if (max_delta < delta)
			max_delta = delta;

		printf ("ANALYSIS: pline %zu need to REDO %zu bytes\n", pline->hashed_id, delta);
    } //end for each line

	printf ("ANALYSIS: min_delta %zu bytes max_delta %zu bytes\n", min_delta, max_delta);
}

/*
 *Start redoer threads to parallelism REDOing
 * */
void 
pm_ppl_redo(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl) {

	uint32_t n, i;
    bool is_err;
	//bool is_multi_redo = false;
	bool is_multi_redo = true;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;
    PMEM_LOG_REDOER*				redoer;
    
	n = ppl->n_buckets;
	// (0) init as in recv_recovery_from_checkpoint_start()
	if (!is_multi_redo){	
		for (i = 0; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);

			/*if a line has recv_diskaddr == ULONG_MAX, that line has all free page block => skip REDO*/
			if (pline->recv_diskaddr == ULONG_MAX)
				continue;

			//test, serialize for easy debug
			bool is_err = pm_ppl_redo_line(pop, ppl, pline);
			assert(!is_err);

			printf("PMEM_RECV: done redoing PHASE 1 line %zu\n", pline->hashed_id);
		}
		return;
	}

	//Parallelism REDO
    //(1) Init the redoers and create threads
    redoer = pm_log_redoer_init(n);
    ppl->redoer = redoer;
	
	redoer->phase = PMEM_REDO_PHASE1;

    redoer->n_remains = n;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

		recv_line = pline->recv_line;
		
		recv_line->mlog_checkpoint_lsn = 0;

        if (pline->recv_diskaddr == ULONG_MAX){
            redoer->n_remains--;
        }
    }
    printf("PMEM_RECV: Need scan and parse %zu lines\n", redoer->n_remains);

    ppl->is_redoing_done = false;
    
    //create threads
    for (i = 0; i < srv_ppl_n_log_flush_threads; ++i) {
        os_thread_create(pm_log_redoer_worker, NULL, NULL);
    }
    
    // (2) Assign pointers
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
        
        //this variable is set true after a thread accept to REDO this line
        pline->is_redoing = false;

        /*if a line has recv_diskaddr == ULONG_MAX, that line has all free page block => skip REDO*/
        if (pline->recv_diskaddr == ULONG_MAX)
            continue;

        //Asign pline to a redoer thread
        redoer->hashed_line_arr[i] = pline;
    }

    //trigger REDOer threads
    os_event_set(redoer->is_log_req_not_empty);
    
    //(3) wait for all threads finish
    while (redoer->n_remains > 0){
        os_event_wait(redoer->is_log_all_finished);
    }

finish: 
    //(4) finish
    pm_log_redoer_close(ppl->redoer);
    printf("\nPMEM_RECV: ===== REDOing completed ======\n");
}

/*
 * Call back funtion from pm_log_redoer_worker()
//Simulate recv_group_scan_log_recs()
// Read all on-disk log rec from start_lsn to the buffer
// Apply REDO
// Return:
// true: error
// false: OK
*/
bool
pm_ppl_redo_line(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline) {
    
    //for phase 1
    uint64_t start_addr, cur_addr, end_addr;

    byte* recv_buf;
    uint64_t recv_buf_len;  //len of the buffer
    uint64_t read_off;  //len of the buffer
    
    //temp variable for memcpy
    byte* src;
    byte* des;
    uint64_t scan_len;
    
    //for phase 2
    bool is_err;
    byte* ptr;
    byte* end_ptr;

    uint64_t len;
    uint32_t actual_len;
    uint32_t n_recs;

	int64_t parsed_recs;
	uint64_t need_recs;
	uint64_t skip1_recs;
	uint64_t skip2_recs;

	uint64_t p1, p2, p3, n1, n2, n3;
	uint64_t s1_1, s1_2, s1_3, s2_1, s2_2, s2_3;

	int64_t total_parsed_recs;
	uint64_t total_need_recs;
	uint64_t total_skip1_recs;
	uint64_t total_skip2_recs;
    dberr_t err;
	
	PMEM_RECV_LINE* recv_line = pline->recv_line;
    PMEM_PAGE_LOG_BUF* plogbuf;
    PMEM_PAGE_LOG_BUF* pcur_logbuf;

    TOID(PMEM_PAGE_LOG_BUF) next_logbuf;
    PMEM_PAGE_LOG_BUF* pnext_logbuf;

	ulint n = ppl->n_buckets;
    start_addr = pline->recv_diskaddr; //this value computed in ANALYSIS step
    end_addr = pline->write_diskaddr;
    cur_addr = start_addr;
	
	assert(recv_line != NULL);
	//init
	//recv_line->recovered_offset tell what is the current global offset of the log file is parsed up to
	//recv_line->recovered_offset = 0;	
	recv_line->recovered_addr = start_addr;	
	recv_line->recovered_offset = 0;	
	recv_line->n_addrs = 0;
	
	//simulate recv_sys_empty_hash()
	pm_ppl_recv_line_empty_hash(pop, ppl, pline);

	recv_line->parse_start_lsn = start_addr;
	recv_line->scanned_lsn = pline->recv_lsn;
	recv_line->recovered_lsn = pline->recv_lsn; // this value computed in ANALYSIS step
	recv_line->scanned_checkpoint_no = 0;

    recv_buf = recv_line->buf;
    recv_buf_len = recv_line->len;

    read_off = 0;

	total_parsed_recs = 0;
	total_need_recs = 0;
	total_skip1_recs = 0;
	total_skip2_recs = 0;
    
    // pline->recv_diskaddr --> pline->write_diskaddr --> flush_logbuf->cur_off --> logbuf->cur_off
read_log_file:
    memset(recv_buf, 0, pline->recv_line->len);

    ///////////////////////////////////////////////////
    // (1) Scan phase: Build the recv buffer
    // ////////////////////////////////////////////////

    if (cur_addr < pline->write_diskaddr){
        //case A: pread n-bytes equal to logbuf size from log file on disk to recv_buf
        //printf("PMEM_INFO redo line %zu read from log file \n", pline->hashed_id);        
        scan_len = pline->write_diskaddr - cur_addr;

        if (scan_len > recv_buf_len)
            scan_len = recv_buf_len;
        else {
            //the last read
            //printf("==> PMEM_INFO the last read in case A scan_len %zu \n", scan_len);
        }
        
        err = pm_log_fil_read(pop, ppl, pline, recv_buf, read_off, scan_len);

        if (err != DB_SUCCESS){
            printf("PMEM_ERROR error in pm_log_fil_read() line %zu", pline->hashed_id);
            assert(0);

			return true;
        }

        //read the actual buffer len (included the header)
        actual_len = mach_read_from_4(recv_buf);
		n_recs = mach_read_from_4(recv_buf + 4);

        assert(actual_len > 0);
        assert(n_recs > 0);

        ptr = recv_buf + PMEM_LOG_BUF_HEADER_SIZE;


        //parse log recs in the recv_buf 
        parsed_recs = pm_ppl_parse_recs(pop, ppl, pline, ptr, actual_len - PMEM_LOG_BUF_HEADER_SIZE, &skip1_recs, &skip2_recs, &need_recs);

		assert(parsed_recs == n_recs);

        if (parsed_recs < 0){
            printf("PMEM_ERROR case A error after pm_ppl_parse_recs() \n");
            is_err = true;
            assert(0);
        }
		total_parsed_recs += parsed_recs;
		total_need_recs += need_recs;
		total_skip1_recs += skip1_recs;
		total_skip2_recs += skip2_recs;


        cur_addr += scan_len;
		recv_line->recovered_addr = cur_addr;
        read_off += scan_len;

        goto read_log_file; 
    }
	p1 = total_parsed_recs;
	n1 = total_need_recs;
	s1_1 = total_skip1_recs;
	s2_1 = total_skip2_recs;


    //If you reach this point, all log recs on disk before write_diskaddr are parsed successful. Now parse log recs from write_diskaddr
    scan_len = 0;
    pcur_logbuf = D_RW(pline->tail_logbuf);

read_log_nvm:
    if (cur_addr < pline->diskaddr){
        /*case B: the flushing from logbuf to log file has not finished when the system crash
		 * memcpy from flushing log buffer in NVDIMM to buf
		 */
        src = ppl->p_align + pcur_logbuf->pmemaddr;
        scan_len = pcur_logbuf->size;

        memcpy(recv_buf, src, scan_len);

        actual_len = mach_read_from_4(recv_buf);
		n_recs = mach_read_from_4(recv_buf + 4);

        assert(actual_len > 0);
        assert(actual_len == pcur_logbuf->cur_off);
		assert(n_recs > 0);
		assert(n_recs == pcur_logbuf->n_recs);

        ptr = recv_buf + PMEM_LOG_BUF_HEADER_SIZE;

        parsed_recs = pm_ppl_parse_recs(pop, ppl, pline, ptr, actual_len - PMEM_LOG_BUF_HEADER_SIZE, &skip1_recs, &skip2_recs, &need_recs);

		assert(parsed_recs == n_recs);

        if (parsed_recs < 0){
            printf("PMEM_ERROR case B error after pm_ppl_parse_recs() \n");
            assert(0);

            return true; 
        }

		total_parsed_recs += parsed_recs;
		total_need_recs += need_recs;
		total_skip1_recs += skip1_recs;
		total_skip2_recs += skip2_recs;
        cur_addr += scan_len;
		recv_line->recovered_addr = cur_addr;

		pnext_logbuf = D_RW(pcur_logbuf->next);

		if ( (pnext_logbuf->diskaddr - pcur_logbuf->diskaddr) > pcur_logbuf->size) {
			printf("!!! WARN: there is a hole log in pline %zu next diskaddr %zu - cur_diskaddr %zu = %zu > logbuf_size %zu, re-read from log file\n",
					recv_line->hashed_id,
					pnext_logbuf->diskaddr,
					pcur_logbuf->diskaddr,
					pnext_logbuf->diskaddr - pcur_logbuf->diskaddr,
					pcur_logbuf->size);
			/*there is hole(s) between the tail and the head
			 * we need to re-read from disk*/
			TOID_ASSIGN(pline->tail_logbuf, (pcur_logbuf->next).oid);
			pline->write_diskaddr = pnext_logbuf->diskaddr;
			goto read_log_file;
		} else {
			pcur_logbuf = pnext_logbuf;
			goto read_log_nvm;	
		}
    }
	p2 = (total_parsed_recs - p1);
	n2 = (total_need_recs - n1);
	s1_2 = (total_skip1_recs - s1_1);
	s2_2 = (total_skip2_recs - s2_1);
	
	/*If you reach here, all on-disk log recs and in-flushing log recs are parsed
	 * now parse the last part of log recs in the logbuf*/

    plogbuf = D_RW(pline->logbuf);
	assert(cur_addr == plogbuf->diskaddr);

    if (plogbuf->cur_off > 0){
        //memcpy from log buffer in NVDIMM to recv_buf
        src = ppl->p_align + plogbuf->pmemaddr;
        scan_len = plogbuf->cur_off;

        memcpy(recv_buf, src, scan_len);
        actual_len = scan_len;
		n_recs = plogbuf->n_recs;

        ptr = recv_buf + PMEM_LOG_BUF_HEADER_SIZE;

        parsed_recs = pm_ppl_parse_recs(pop, ppl, pline, ptr, actual_len - PMEM_LOG_BUF_HEADER_SIZE, &skip1_recs, &skip2_recs, &need_recs);

		assert(parsed_recs == n_recs);

        if (parsed_recs < 0){
            printf("PMEM_ERROR case C error after pm_ppl_parse_recs() \n");
            assert(0);

            return true; 
        }

		total_parsed_recs += parsed_recs;
		total_need_recs += need_recs;
		total_skip1_recs += skip1_recs;
		total_skip2_recs += skip2_recs;
        cur_addr += scan_len;
		recv_line->recovered_addr = cur_addr;
    }
	p3 = (total_parsed_recs - p1 - p2);
	n3 = (total_need_recs - n1 - n2);
	s1_3 = total_skip1_recs - s1_1 - s1_2;
	s2_3 = total_skip2_recs - s2_1 - s2_2;

	printf("PMEM_INFO: FINISH parse line %zu need/parsed %zu/%zu n1/s1_1/s2_1/p1 %zu/%zu/%zu/%zu n2/s1_2/s2_2/p2 %zu/%zu/%zu/%zu n3/s1_3/s2_3/p3 %zu/%zu/%zu/%zu \n",
		   	pline->hashed_id,
		   	total_need_recs, total_parsed_recs,
			n1, s1_1, s2_1, p1,
			n2, s1_2, s2_2, p2,
			n3, s1_3, s2_3, p3);

    return false; 
}

/*
 * Alternative to recv_parse_log_recs()
 * Parse log recs on a line from start_ptr to start_ptr + parse_len
 *
 *Because we don't use checkpoint in PPL
 We do not distinguish single_rec and multi_rec
 @param[in] pop
 @param[in] ppl
 @param[in] pline
 @param[in] start_ptr pointer to the start of logbuf
 @param[in] parse_len number bytes should be parsed
 @param[out] n_need_recs number of log recs need to add to hashtable after parsing
 @return: number of parsed recs
 * */
int64_t
pm_ppl_parse_recs(
		PMEMobjpool*		        pop,
		PMEM_PAGE_PART_LOG*	        ppl,
		PMEM_PAGE_LOG_HASHED_LINE*  pline,
		byte*                       start_ptr,
		uint64_t                    parse_len,
		uint64_t*					n_skip1_recs,
		uint64_t*					n_skip2_recs,
		uint64_t*					n_need_recs
		)
{
    byte* ptr;
    byte* end_ptr;
	bool		single_rec;
	mlog_id_t	type;
	ulint		space;
	ulint		page_no;
	byte*		body;

	lsn_t		new_recovered_lsn;
	lsn_t		old_lsn;
    uint64_t len;
    uint64_t cur_off;
	uint64_t rec_lsn;

	uint64_t n_parsed_recs;

	bool	is_need;

	PMEM_RECV_LINE* recv_line = pline->recv_line;
	store_t		store = STORE_YES;
    end_ptr = start_ptr + parse_len;
	PMEM_PARSE_RESULT parse_res;

    cur_off = 0;
	recv_line->recovered_offset = cur_off;

	n_parsed_recs = 0;
	*n_need_recs = 0;
	*n_skip1_recs = *n_skip2_recs = 0;

//parse each log recs, increase the recovered_off after each loop
loop:
    //ptr = start_ptr + recv_line->recovered_offset;
    ptr = start_ptr + cur_off;

    if (ptr == end_ptr) {
        //return false; //done, all log recs are scanned
        return n_parsed_recs; //done, all log recs are scanned
    }
	is_need = false;

	old_lsn = recv_line->recovered_lsn;
	
	//parse a log rec from ptr, return rec len
	len = pm_ppl_recv_parse_log_rec(
			pop, ppl, recv_line,
			&type, ptr, end_ptr, &space,
			&page_no, true, &rec_lsn, &body,
			&parse_res, &is_need);

	switch (parse_res){
		case PMEM_PARSE_BLOCK_NOT_EXISTED:
			*n_skip1_recs = *n_skip1_recs + 1;
			break;
		case PMEM_PARSE_LSN_OLD:
			*n_skip2_recs = *n_skip2_recs + 1;
			break;
		case PMEM_PARSE_NEED:
		default:
			assert(is_need);
			break;
	}

	//test, the correctness of is_need, redundant REDO is better than wrong REDO
	//is_need = true;
	//end test
	
	recv_line->found_corrupt_log = recv_sys->found_corrupt_log;
	recv_line->found_corrupt_fs = recv_sys->found_corrupt_fs;
	
	if (len == 0) {
		//return(false);
		assert(0);
        return n_parsed_recs; // reach MLOG_MULTI_REC_END
	}

	if (recv_line->found_corrupt_log) {
		recv_report_corrupt_log(
				ptr, type, space, page_no);
		//return(true);
		return -1;
	}

	if (recv_line->found_corrupt_fs) {
		//return(true);
		return -1;
	}

	//update
	n_parsed_recs++;
	cur_off += len;
	recv_line->recovered_offset = cur_off;

	//Note: if the rec_lsn < recv_line->recovered_lsn, this log rec has already applied in the database, we don't need it
	//if (rec_lsn < recv_line->recovered_lsn){
	//	if (is_need){
	//		printf("pline %zu cur_off %zu rec_lsn %zu < recv_line->recovered_lsn %zu, but is_need is true, logical error!!!\n", pline->hashed_id, cur_off, rec_lsn, recv_line->recovered_lsn);
	//	}
	//	//assert(!is_need);
	//}
	//else{
	//	//recv_line->recovered_lsn = rec_lsn;
	//}
	
	/*update recovered_lsn, this value is need in rebuilding tone page in DWB*/	
	recv_line->recovered_lsn = rec_lsn;
	//add the parsed log record to HASH TABLE in apply phase
	switch (type) {
		lsn_t	lsn;
		case MLOG_MULTI_REC_END:
		//we simply skip and don't handle this as original
			goto loop;
		case MLOG_DUMMY_RECORD:
		/* Do nothing */
		break;
		case MLOG_CHECKPOINT:
		printf("PMEM_ERROR: we don't call log_checkpoint() in PPL, check againn");
		break;
		case MLOG_FILE_NAME:
		case MLOG_FILE_DELETE:
		case MLOG_FILE_CREATE2:
		case MLOG_FILE_RENAME2:
		case MLOG_INDEX_LOAD:
		case MLOG_TRUNCATE:
		/* These were already handled by
		   recv_parse_log_rec() and
		   recv_parse_or_apply_log_rec_body(). */
		break;
		default:
		switch (store) {
			case STORE_NO:
				break;
			case STORE_IF_EXISTS:
				if (fil_space_get_flags(space)
						== ULINT_UNDEFINED) {
					break;
				}
				/* fall through */
			case STORE_YES:
				if (is_need){
					if (!IS_GLOBAL_HASHTABLE){
						pm_ppl_recv_add_to_hash_table(
								pop, ppl, recv_line,
								type, space, page_no,
								body, ptr + len,
								rec_lsn, rec_lsn + len);
						*n_need_recs = *n_need_recs + 1;
					} else {
						/*the global hashtable approach*/
						pmemobj_rwlock_wrlock(pop, &ppl->lock);
						/*put the recv_addr to the global hashtable in ppl->recv_line.
						 * Contention between redoer workers
						 * */
						pm_ppl_recv_add_to_hash_table(
								pop, ppl, ppl->recv_line,
								type, space, page_no,
								body, ptr + len,
								rec_lsn, rec_lsn + len);

						*n_need_recs = *n_need_recs + 1;

						pmemobj_rwlock_unlock(pop, &ppl->lock);
					}
				}
				break;
		}
		/* fall through */
	}

	goto loop;
}

/*
 * Simulate recv_parse_log_rec()
 *Note that in PPL, the structure of log record header has 
 extra 2-byte for rec_len (type, space, page_no, rec_len, body)
@param[in]	pop		pop
@param[in]	ppl		partition page log
@param[in]	recv_line	recv_line
@param[out]	type		log record type
@param[in]	ptr		pointer to a log rec on recv_line->buf
@param[in]	end_ptr		end of the buffer in the recv_line
@param[out]	space_id	tablespace identifier
@param[out]	page_no		page number
@param[in]	apply		whether to apply MLOG_FILE_* records
@param [out] rec_lsn	lsn of the log record
@param[out]	body		start of log record body
@param[out] parse_res	parsed response (1: need, 2: plogblock is not existed, 3: 
@param[out]	is_need		true if we need to insert this log rec in hashtable.
 * */
uint64_t
pm_ppl_recv_parse_log_rec(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_RECV_LINE* recv_line,
	mlog_id_t*	type,
	byte*		ptr,
	byte*		end_ptr,
	ulint*		space,
	ulint*		page_no,
	bool		apply,
	uint64_t*	rec_lsn,
	byte**		body,
	PMEM_PARSE_RESULT*	parse_res,
	bool*		is_need) {
	
	PMEM_PAGE_LOG_BLOCK* plog_block;

	uint32_t rec_len;

	byte*	new_ptr;
	
	*is_need = false;
	*body = NULL;
	
	//(1) Do some checks
	UNIV_MEM_INVALID(type, sizeof *type);
	UNIV_MEM_INVALID(space, sizeof *space);
	UNIV_MEM_INVALID(page_no, sizeof *page_no);
	UNIV_MEM_INVALID(body, sizeof *body);

	if (ptr == end_ptr) {
		assert(0);
		return(0);
	}
	
	//skip those log types
	switch (*ptr) {
	case MLOG_MULTI_REC_END:
	case MLOG_DUMMY_RECORD:
		*type = static_cast<mlog_id_t>(*ptr);
		return(1);
	case MLOG_CHECKPOINT:
		if (end_ptr < ptr + SIZE_OF_MLOG_CHECKPOINT) {
			assert(0);
			return(0);
		}
		*type = static_cast<mlog_id_t>(*ptr);
		return(SIZE_OF_MLOG_CHECKPOINT);
	case MLOG_MULTI_REC_END | MLOG_SINGLE_REC_FLAG:
	case MLOG_DUMMY_RECORD | MLOG_SINGLE_REC_FLAG:
	case MLOG_CHECKPOINT | MLOG_SINGLE_REC_FLAG:
		recv_line->found_corrupt_log = true;
		assert(0);
		return(0);
	}//end switch
	
	//(2) Parse the log header to get space, page_no
		
	//get the header of the log record, the return pointer should point to "rec_len"
	new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);
	
	rec_len = mach_read_from_2(new_ptr);
	assert(rec_len > 0);
	new_ptr += 2;

	*rec_lsn = mach_read_from_8(new_ptr);
	new_ptr += 8;
	
	*body = new_ptr;
	//debug UNDO bug
	//if (*type == MLOG_UNDO_HDR_CREATE &&
	//	   	*page_no == 28796) {
	//	const byte** temp_ptr = (const byte**) body;
	//	trx_id_t    trx_id = mach_u64_parse_compressed(temp_ptr, end_ptr);
	//	printf("parse MLOG_UNDO_HDR_CREATE page %zu tid %zu\n", *page_no, trx_id);

	//	*body = new_ptr;
	//}
	//(3) Varify whether this log rec is need
	ulint key;
	PMEM_FOLD(key, *space, *page_no);

	plog_block = pm_ppl_get_log_block_by_key(pop, ppl, key);
	if (plog_block == NULL){
		//the page (space, page_no) has been flushed to disk before the crash
		//do not need recover this page
		*parse_res = PMEM_PARSE_BLOCK_NOT_EXISTED;
		return rec_len;
	}

	assert(plog_block->key == key);	
	
	if (plog_block->state != PMEM_IN_USED_BLOCK){
		printf("~~ WARN pm_ppl_recv_parse_log_rec() plog_block (%zu, %zu) has state %zu\n", *space, *page_no, plog_block->state);
		assert(0);
	}


	//test, skip this
	if ((*rec_lsn) < plog_block->firstLSN){
		//this log rec is old, skip it
		*parse_res = PMEM_PARSE_LSN_OLD;
		return rec_len;
	}

	/*we strictly check the first log rec here
	 * the plog_block->first_rec_found is set false at init and only set true here, when the first log rec is found
	 * */
	if (!plog_block->first_rec_found) {
		assert(plog_block->first_rec_size == rec_len);
		assert(plog_block->first_rec_type == *type);
		
		/*There is a case one page is flush then read again many times, the info in the plog_block is the last read*/
		//if plog_block->firstLSN < *rec_lsn, we miss some log recs of this plog_block
		assert(plog_block->firstLSN == *rec_lsn);
		assert(plog_block->start_diskaddr == recv_line->recovered_addr);
		assert(plog_block->start_off == (recv_line->recovered_offset + PMEM_LOG_BUF_HEADER_SIZE));
		//all checks are passed
		plog_block->first_rec_found = true;	
	}

	//(4) Parse the body as original InnoDB

	if (UNIV_UNLIKELY(!new_ptr)) {
		assert(0);
		return(0);
	}
	
	//we reuse this function, at this time only parse, not apply. The return pointer is the next byte after the body
	new_ptr = recv_parse_or_apply_log_rec_body(
		*type, new_ptr, end_ptr, *space, *page_no, NULL, NULL);

	if (recv_line != NULL){
		recv_line->found_corrupt_log = recv_sys->found_corrupt_log;
		recv_line->found_corrupt_fs = recv_sys->found_corrupt_fs;
	}

	if (UNIV_UNLIKELY(new_ptr == NULL)) {
		assert(0);
		return(0);
	}

	//check ( (new_ptr - ptr) == rec_len)
	if ( (new_ptr - ptr) != rec_len) {
		printf("PMEM_WARN parsed len %zu differ to check len %zu at rec type %zu space %zu page %zu\n", (new_ptr - ptr), rec_len, *type, *space, *page_no);
		assert ( (new_ptr - ptr) == rec_len);
	}

	*is_need = true;
	*parse_res = PMEM_PARSE_NEED;

	return(new_ptr - ptr);
	//return(rec_len);
}

void 
pm_ppl_recv_line_empty_hash(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline)
{
	uint64_t size;	
	PMEM_RECV_LINE* recv_line;
	ulint avail_mem;

	avail_mem = buf_pool_get_curr_size();
	
	if (pline == NULL){
		recv_line = ppl->recv_line;
		size = avail_mem / 512;
	}
	else {
		recv_line = pline->recv_line;
		size = D_RW(pline->logbuf)->size;
	}


	hash_table_free(recv_line->addr_hash);
	mem_heap_empty(recv_line->heap);

	recv_line->addr_hash = hash_create(size);
}

/*simulate recv_add_to_hash_table()
 *The caller reponse for holding the lock of hashtable
 * */
void
pm_ppl_recv_add_to_hash_table(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_RECV_LINE* recv_line,
	mlog_id_t	type,		/*!< in: log record type */
	ulint		space,		/*!< in: space id */
	ulint		page_no,	/*!< in: page number */
	byte*		body,		/*!< in: log record body */
	byte*		rec_end,	/*!< in: log record end */
	lsn_t		start_lsn,
	lsn_t		end_lsn) 
{
	recv_t*		recv;
	ulint		len;
	recv_data_t*	recv_data;
	recv_data_t**	prev_field;
	recv_addr_t*	recv_addr;

	ut_ad(type != MLOG_FILE_DELETE);
	ut_ad(type != MLOG_FILE_CREATE2);
	ut_ad(type != MLOG_FILE_RENAME2);
	ut_ad(type != MLOG_FILE_NAME);
	ut_ad(type != MLOG_DUMMY_RECORD);
	ut_ad(type != MLOG_CHECKPOINT);
	ut_ad(type != MLOG_INDEX_LOAD);
	ut_ad(type != MLOG_TRUNCATE);

	len = rec_end - body;
	
	//(1) allocate recv obj to capture the log record
	recv = static_cast<recv_t*>(
		mem_heap_alloc(recv_line->heap, sizeof(recv_t)));
	//recv = static_cast<recv_t*>(
	//	malloc(sizeof(recv_t)));

	recv->type = type;
	recv->len = rec_end - body;
	recv->start_lsn = start_lsn;
	recv->end_lsn = end_lsn;
	
	if (recv->len == 0){
		assert(type == MLOG_INIT_FILE_PAGE2 
				|| type == MLOG_COMP_PAGE_CREATE
				|| type == MLOG_IBUF_BITMAP_INIT);
	}	

	// (2) get the recv_addr_t from the hash table in this recv_line
	// or create the new one

	recv_addr = pm_ppl_recv_get_fil_addr_struct(recv_line, space, page_no);

	if (recv_addr == NULL) {

		recv_addr = static_cast<recv_addr_t*>(
			mem_heap_alloc(recv_line->heap, sizeof(recv_addr_t)));
		//recv_addr = static_cast<recv_addr_t*>(
		//	malloc(sizeof(recv_addr_t)));

		recv_addr->space = space;
		recv_addr->page_no = page_no;
		recv_addr->state = RECV_NOT_PROCESSED;

		UT_LIST_INIT(recv_addr->rec_list, &recv_t::rec_list);

		HASH_INSERT(
				recv_addr_t, addr_hash,
			   	recv_line->addr_hash,
			    recv_fold(space, page_no),
			   	recv_addr);
		recv_line->n_addrs++;

		//tdnguyen test how many UNDO pages
		if (type == MLOG_UNDO_INSERT){
		  printf("==> PMEM_RECV: UNDO page added to HT, space %zu page %zu\n", space, page_no);
		}
	}

	UT_LIST_ADD_LAST(recv_addr->rec_list, recv);
	
	//(3) recv_addr only has the header, now link it with the log record's body
	prev_field = &(recv->data);
	/* Store the log record body in chunks of less than UNIV_PAGE_SIZE: recv_line->heap grows into the buffer pool, and bigger chunks could not
	be allocated */

	while (rec_end > body) {

		len = rec_end - body;

		if (len > RECV_DATA_BLOCK_SIZE) {
			len = RECV_DATA_BLOCK_SIZE;
		}

		recv_data = static_cast<recv_data_t*>(
			mem_heap_alloc(recv_line->heap,
				       sizeof(recv_data_t) + len));
		//recv_data = static_cast<recv_data_t*>(
		//	malloc(sizeof(recv_data_t) + len));

		*prev_field = recv_data;

		memcpy(recv_data + 1, body, len);

		prev_field = &(recv_data->next);

		body += len;
	}

	*prev_field = NULL;
}
/*
 * Alternative for log_sys->lsn
 * */
ulint
pm_ppl_get_max_lsn(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl)
{
	uint32_t n, k, i, j;
	uint64_t max_lsn;

	max_lsn = 0;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

    //find low_watermark for each line
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

        //for each block in the line
        for (j = 0; j < k; j++){
            plog_block = D_RW(D_RW(pline->arr)[j]);

            if (!plog_block->is_free){
				if (max_lsn < plog_block->lastLSN){
					max_lsn = plog_block->lastLSN;
				}
            }
        } //end for each block in the line

    } //end for each line

	return max_lsn;
}

ulint
pm_ppl_recv_get_max_recovered_lsn(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl)
{
	uint32_t n, i;
	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;
	
	ulint max_lsn = 0;
	
	n = ppl->n_buckets;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		recv_line = pline->recv_line;

		if (max_lsn < recv_line->recovered_lsn){
			max_lsn = recv_line->recovered_lsn;
		}
	}

	return max_lsn;
}

/*
 * simulate recv_init_crash_recovery_spaces()
 * @param pop[in]
 * @param ppl[in]
 * @param max_recovered_lsn: max last lsn in recv_line, copute from REDO phase 1
 *
 * */
dberr_t
pm_ppl_recv_init_crash_recovery_spaces(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	ulint max_recovered_lsn)
{
	uint32_t n, i;
	
	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;

	typedef std::set<ulint>	space_set_t;
	bool		flag_deleted	= false;
	space_set_t	missing_spaces;

	ut_ad(!srv_read_only_mode);
	ut_ad(recv_needed_recovery);

	ib::info() << "Database was not shutdown normally!";
	ib::info() << "Starting crash recovery.";

	for (recv_spaces_t::iterator i = recv_spaces.begin();
	     i != recv_spaces.end(); i++) {
		ut_ad(!is_predefined_tablespace(i->first));

		if (i->second.deleted) {
			/* The tablespace was deleted,
			so we can ignore any redo log for it. */
			flag_deleted = true;
		} else if (i->second.space != NULL) {
			/* The tablespace was found, and there
			are some redo log records for it. */
			//Simulate fil_names_dirty()
			pm_ppl_fil_names_dirty(pop, ppl, max_recovered_lsn, i->second.space);
		} else {
			missing_spaces.insert(i->first);
			flag_deleted = true;
		}
	}

	if (flag_deleted) {
		printf("PMEM_INFO flag_deleted in pm_ppl_recv_init_crash_recovery_spaces()\n");

		for (i = 0; i < n; i++){
			pline = D_RW(D_RW(ppl->buckets)[i]);
			recv_line = pline->recv_line;

			dberr_t err = DB_SUCCESS;

			for (ulint h = 0;
					h < hash_get_n_cells(recv_line->addr_hash);
					h++) {
				for (recv_addr_t* recv_addr
						= static_cast<recv_addr_t*>(
							HASH_GET_FIRST(
								recv_line->addr_hash, h));
						recv_addr != 0;
						recv_addr = static_cast<recv_addr_t*>(
							HASH_GET_NEXT(addr_hash, recv_addr))) {
					const ulint space = recv_addr->space;

					if (is_predefined_tablespace(space)) {
						continue;
					}

					recv_spaces_t::iterator i
						= recv_spaces.find(space);

					if (i == recv_spaces.end()) {
						recv_init_missing_mlog(recv_addr);
						recv_addr->state = RECV_DISCARDED;
						continue;
					}

					if (i->second.deleted) {
						ut_ad(missing_spaces.find(space)
								== missing_spaces.end());
						recv_addr->state = RECV_DISCARDED;
						continue;
					}

					space_set_t::iterator m = missing_spaces.find(
							space);

					if (m != missing_spaces.end()) {
						missing_spaces.erase(m);
						err = recv_init_missing_space(err, i);
						recv_addr->state = RECV_DISCARDED;
						/* All further redo log for this
						   tablespace should be removed. */
						i->second.deleted = true;
					}
				}
			}//end for each bucket in recv_line

			if (err != DB_SUCCESS) {
				return(err);
			}
		}//end for each recv_line in ppl
	}

	for (space_set_t::const_iterator m = missing_spaces.begin();
	     m != missing_spaces.end(); m++) {
		recv_spaces_t::iterator i = recv_spaces.find(*m);
		ut_ad(i != recv_spaces.end());

		ib::info() << "Tablespace " << i->first
			<< " was not found at '" << i->second.name
			<< "', but there were no modifications either.";
	}
#if defined (UNIV_PMEMOBJ_BUF)
	//We don't need the torn page correction process, skip this 
#else //original
	buf_dblwr_process();
#endif
	if (srv_force_recovery < SRV_FORCE_NO_LOG_REDO) {
		/* Spawn the background thread to flush dirty pages
		from the buffer pools. */
		os_thread_create(recv_writer_thread, 0, 0);
	}

	return(DB_SUCCESS);

}

/*
 *simulate recv_get_fil_addr_struct()
 * */
recv_addr_t*
pm_ppl_recv_get_fil_addr_struct(
	PMEM_RECV_LINE* recv_line,
	ulint	space,
	ulint	page_no) 
{
	recv_addr_t*	recv_addr;
	ulint hashed;
	
//	hashed = pm_ppl_recv_hash(recv_line, space, page_no);
//	
//	//test
//	recv_addr_t* temp = (recv_addr_t*) HASH_GET_FIRST(recv_line->addr_hash, 1100);
//
//	recv_addr_t* temp2 = (recv_addr_t*) (recv_line->addr_hash->array + hashed);
//
//	recv_addr = (recv_addr_t*) HASH_GET_FIRST(recv_line->addr_hash, hashed);
//
//	while (recv_addr != NULL){
//		if (recv_addr->space == space
//		    && recv_addr->page_no == page_no) {
//
//			return(recv_addr);
//		}
//
//		recv_addr = (recv_addr_t*) HASH_GET_NEXT(addr_hash, recv_addr);
//	}
	for (recv_addr = static_cast<recv_addr_t*>(
			HASH_GET_FIRST(recv_line->addr_hash,
				       pm_ppl_recv_hash(recv_line, space, page_no)));
	     recv_addr != 0;
	     recv_addr = static_cast<recv_addr_t*>(
		     HASH_GET_NEXT(addr_hash, recv_addr))) {

		if (recv_addr->space == space
		    && recv_addr->page_no == page_no) {

			return(recv_addr);
		}
	}

	return(NULL);
}

/*
 *Simulate recv_recover_page_func()
 Note that this function is called from two different threads
 Thread 1: recovery thread in pm_ppl_recv_apply_hashed_line -> pm_ppl_recv_recover_page_func() 
 Thread 2: IO thread -> fil_io_wait() -> buf_page_io_complete() -> recv_recover_page_func() -> pm_ppl_recv_recover_page_func()

 So we need to lock the line 
 * */
void
pm_ppl_recv_recover_page_func(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ibool		just_read_in,
				/*!< in: TRUE if the i/o handler calls
				this for a freshly read page */
	buf_block_t*	block)	/*!< in/out: buffer block */
{
	PMEM_RECV_LINE* recv_line;
	PMEM_PAGE_LOG_BLOCK* plog_block;
	ulint key;

	page_t*		page;
	page_zip_des_t*	page_zip;
	recv_addr_t*	recv_addr;
	recv_t*		recv;
	recv_t*		prev_recv;

	byte*		buf;
	lsn_t		start_lsn;
	lsn_t		end_lsn;
	lsn_t		page_lsn;
	lsn_t		page_newest_lsn;
	ibool		modification_to_page;
	mtr_t		mtr;
	
	bool is_gb_ht = (pline == NULL);
	
	//for debug
	uint32_t page_no = block->page.id.page_no();

	if(is_gb_ht){	
		pmemobj_rwlock_wrlock(pop, &ppl->lock);	
		recv_line = ppl->recv_line;
	}
	else {
		pmemobj_rwlock_wrlock(pop, &pline->lock);	
		recv_line = pline->recv_line;
	}

	if (recv_line->apply_log_recs == FALSE) {
		/* Log records should not be applied now */

		if(is_gb_ht){	
			pmemobj_rwlock_unlock(pop, &ppl->lock);	
		}
		else{
			pmemobj_rwlock_unlock(pop, &pline->lock);	
		}
		return;
	}

	//check read space_id and read page_no
	page = block->frame;

	ulint read_space_id = mach_read_from_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
	ulint read_page_no = mach_read_from_4(page + FIL_PAGE_OFFSET);
	
//	if(read_space_id != block->page.id.space() || read_page_no != block->page.id.page_no()){
//		//printf("PMEM_ERROR in pm_ppl_recv_recover_page_func(), input (space %zu, page_no %zu) differ read (space %zu, page_no %zu)\n", block->page.id.space(), block->page.id.page_no(), read_space_id, read_page_no);
//		//recv_line->n_addrs--;
//		if (recv_line->skip_zero_page){
//			if(is_gb_ht){	
//				pmemobj_rwlock_unlock(pop, &ppl->lock);	
//			}
//			else{
//				pmemobj_rwlock_unlock(pop, &pline->lock);	
//			}
//			return;
//		}
//	}

	recv_addr = pm_ppl_recv_get_fil_addr_struct(
			recv_line,
			block->page.id.space(),
			block->page.id.page_no());

	if ((recv_addr == NULL)
	    || (recv_addr->state == RECV_BEING_PROCESSED)
	    || (recv_addr->state == RECV_PROCESSED)) {
		ut_ad(recv_addr == NULL || recv_needed_recovery);

		if(is_gb_ht){	
			pmemobj_rwlock_unlock(pop, &ppl->lock);	
		}
		else{
			pmemobj_rwlock_unlock(pop, &pline->lock);	
		}
		return;
	}
#ifndef UNIV_HOTBACKUP
	ut_ad(recv_needed_recovery);

	DBUG_PRINT("ib_log",
		   ("Applying log to page %u:%u",
		    recv_addr->space, recv_addr->page_no));
#endif /* !UNIV_HOTBACKUP */
	recv_addr->state = RECV_BEING_PROCESSED;

	mtr_start(&mtr);
	mtr_set_log_mode(&mtr, MTR_LOG_NONE);

	page = block->frame;
	page_zip = buf_block_get_page_zip(block);

	/* the role of plogblock here is for testing only
	 * get pmem_log_block*/
	PMEM_FOLD(key, recv_addr->space, recv_addr->page_no);
	plog_block = pm_ppl_get_log_block_by_key(pop, ppl, key);
	//assert(plog_block != NULL);
	

	if (just_read_in) {
		/* Move the ownership of the x-latch on the page to
		this OS thread, so that we can acquire a second
		x-latch on it.  This is needed for the operations to
		the page to pass the debug checks. */

		rw_lock_x_lock_move_ownership(&block->lock);
	}

	ibool	success = buf_page_get_known_nowait(
		RW_X_LATCH, block, BUF_KEEP_OLD,
		__FILE__, __LINE__, &mtr);
	ut_a(success);

	buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

	/* Read the newest modification lsn from the page */
	page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);

	page_newest_lsn = buf_page_get_newest_modification(&block->page);

	if (page_newest_lsn) {

		page_lsn = page_newest_lsn;
	}

	//assert(page_lsn <= plog_block->firstLSN);

	modification_to_page = FALSE;
	start_lsn = end_lsn = 0;
	
	fil_space_t* space = fil_space_get(recv_addr->space);
	assert(space != NULL);

	uint32_t count = 0;	
	//printf("===> START apply REDO log for read page (%zu %zu)\n", recv_addr->space, recv_addr->page_no);

	recv = UT_LIST_GET_FIRST(recv_addr->rec_list);
	
	//parse and apply each recv in the hashtable
	ulint prev_start_lsn = 0;

	prev_recv = NULL;	

	while (recv) {
		if (prev_recv != NULL){
			assert(prev_recv->start_lsn <= recv->start_lsn);
		}
		//don't use end_lsn
		//end_lsn = recv->end_lsn;

		if (recv->len > RECV_DATA_BLOCK_SIZE) {
			/* We have to copy the record body to a separate
			buffer */

			buf = static_cast<byte*>(ut_malloc_nokey(recv->len));

			recv_data_copy_to_buf(buf, recv);
		} else {
			buf = ((byte*)(recv->data)) + sizeof(recv_data_t);
		}

		if (recv->type == MLOG_INIT_FILE_PAGE) {
			page_lsn = page_newest_lsn;

			memset(FIL_PAGE_LSN + page, 0, 8);
			memset(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM
			       + page, 0, 8);

			if (page_zip) {
				memset(FIL_PAGE_LSN + page_zip->data, 0, 8);
			}
		}

		/* If per-table tablespace was truncated and there exist REDO
		records before truncate that are to be applied as part of
		recovery (checkpoint didn't happen since truncate was done)
		skip such records using lsn check as they may not stand valid
		post truncate.
		LSN at start of truncate is recorded and any redo record
		with LSN less than recorded LSN is skipped.
		Note: We can't skip complete recv_addr as same page may have
		valid REDO records post truncate those needs to be applied. */
		bool	skip_recv = false;

		if (srv_was_tablespace_truncated(space)) {
			lsn_t	init_lsn =
				truncate_t::get_truncated_tablespace_init_lsn(
				recv_addr->space);
			skip_recv = (recv->start_lsn < init_lsn);
		}

		/* Ignore applying the redo logs for tablespace that is
		truncated. Post recovery there is fixup action that will
		restore the tablespace back to normal state.
		Applying redo at this stage can result in error given that
		redo will have action recorded on page before tablespace
		was re-inited and that would lead to an error while applying
		such action. */

		if (recv->start_lsn >= page_lsn
		    && !srv_is_tablespace_truncated(recv_addr->space)
		    && !skip_recv) {

			//lsn_t	end_lsn;

			if (!modification_to_page) {

				modification_to_page = TRUE;
				start_lsn = recv->start_lsn;
			}

			recv_parse_or_apply_log_rec_body(recv->type, buf, buf + recv->len, recv_addr->space, recv_addr->page_no, block, &mtr);

			recv_line->found_corrupt_log = recv_sys->found_corrupt_log;
			recv_line->found_corrupt_fs = recv_sys->found_corrupt_fs;

			//end_lsn = recv->start_lsn + recv->len;
			//mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);
			//mach_write_to_8(UNIV_PAGE_SIZE
			//		- FIL_PAGE_END_LSN_OLD_CHKSUM
			//		+ page, end_lsn);

			//if (page_zip) {
			//	mach_write_to_8(FIL_PAGE_LSN
			//			+ page_zip->data, end_lsn);
			//}
			
			//in PPL we write start_lsn on page header
			mach_write_to_8(FIL_PAGE_LSN + page, start_lsn);
			mach_write_to_8(UNIV_PAGE_SIZE
					- FIL_PAGE_END_LSN_OLD_CHKSUM
					+ page, start_lsn);

			if (page_zip) {
				mach_write_to_8(FIL_PAGE_LSN
						+ page_zip->data, start_lsn);
			}
		} //end check apply log rec

		if (recv->len > RECV_DATA_BLOCK_SIZE) {
			ut_free(buf);
		}
		
		prev_start_lsn = recv->start_lsn;
		prev_recv = recv;

		recv = UT_LIST_GET_NEXT(rec_list, recv);

		count++;
	} //end while(recv)

	//printf("===> END apply REDO log for read page (%zu %zu) %zu recs \n", recv_addr->space, recv_addr->page_no, count);

	if (modification_to_page) {
		ut_a(block);

		log_flush_order_mutex_enter();
		//buf_flush_recv_note_modification(block, start_lsn, end_lsn);
		buf_flush_recv_note_modification(block, start_lsn, start_lsn);
		log_flush_order_mutex_exit();
	}

	/* Make sure that committing mtr does not change the modification lsn values of page */

	mtr.discard_modifications();

	mtr_commit(&mtr);

	if (recv_max_page_lsn < page_lsn) {
		recv_max_page_lsn = page_lsn;
	}

	recv_addr->state = RECV_PROCESSED;

	ut_a(recv_line->n_addrs);
	recv_line->n_addrs--;

	if(is_gb_ht){	
		pmemobj_rwlock_unlock(pop, &ppl->lock);	
	}
	else{
		pmemobj_rwlock_unlock(pop, &pline->lock);	
	}
}

ulint
pm_ppl_recv_read_in_area(
	PMEM_RECV_LINE* recv_line,
	const page_id_t&	page_id)
{
	recv_addr_t* recv_addr;
	ulint	page_nos[RECV_READ_AHEAD_AREA];
	ulint	low_limit;
	ulint	n;

	low_limit = page_id.page_no()
		- (page_id.page_no() % RECV_READ_AHEAD_AREA);

	n = 0;

	for (ulint page_no = low_limit;
	     page_no < low_limit + RECV_READ_AHEAD_AREA;
	     page_no++) {

		recv_addr = pm_ppl_recv_get_fil_addr_struct(recv_line, page_id.space(), page_no);

		const page_id_t	cur_page_id(page_id.space(), page_no);

		if (recv_addr && !buf_page_peek(cur_page_id)) {


			if (recv_addr->state == RECV_NOT_PROCESSED) {
				recv_addr->state = RECV_BEING_READ;

				page_nos[n] = page_no;

				n++;
			}
		}
	}

	buf_read_recv_pages(FALSE, page_id.space(), page_nos, n);

	return(n);
}
///////////////// APPLY PHASE //////////////
void
pm_ppl_recv_apply_single_page(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ulint space, ulint page_no)
{

	mtr_t	mtr;

	const page_id_t		page_id(space, page_no);
	bool			found;
	const page_size_t&	page_size
		= fil_space_get_page_size(space, &found);

	ut_ad(found);
	if (buf_page_peek(page_id)) {
		buf_block_t*	block;

		mtr_start(&mtr);

		block = buf_page_get(
				page_id, page_size,
				RW_X_LATCH, &mtr);

		buf_block_dbg_add_level(
				block, SYNC_NO_ORDER_CHECK);

		pm_ppl_recv_recover_page_func( 
				pop, ppl, pline, FALSE, block);

		mtr_commit(&mtr);
	} else {
		// apply is done in IO thread after this read call is called
		bool read_ok = buf_read_page(
				page_id, page_size);

		assert(read_ok);
	}
}

/*
 *Apply important pages prior to apply other 
 For system tablespace
 1) page 0
 2) trx_sys page (page 5)
 For each dirty space we apply
 1) page 0
 2) INODE (page 2)
 3) root clustered index page
 4) root non-clustered index page
 * */

void
pm_ppl_recv_apply_prior_pages(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ibool	allow_ibuf)
{
	fil_space_t* space;
	recv_addr_t* recv_addr;
	uint32_t j;
	
	PMEM_RECV_LINE* recv_line;

	uint32_t page_no_arr[] = {0, 2, 3, 4};
	uint32_t page_no_arr_len = 4;

	if (pline != NULL){ 
		recv_line	= pline->recv_line;
	} else {
		recv_line = ppl->recv_line;
	}

	recv_line->apply_log_recs = TRUE;
	recv_line->apply_batch_on = TRUE;

	//System tablespace
	recv_addr = pm_ppl_recv_get_fil_addr_struct(recv_line, 0, 0);
	if (recv_addr != NULL){
		printf("Apply space 0 page 0\n");
		pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
	}

	recv_addr = pm_ppl_recv_get_fil_addr_struct(recv_line, 0, 2);
	if (recv_addr != NULL){
		printf("Apply space 0 page 2 (INODE)\n");
		pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
	}

	recv_addr = pm_ppl_recv_get_fil_addr_struct(recv_line, 0, 5);
	if (recv_addr != NULL){
		printf("Apply space 0 page 5 (SYS_TRX)\n");
		pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
	}
	
	//User tablespaces
	for (recv_spaces_t::iterator i = recv_spaces.begin();
	     i != recv_spaces.end(); i++) {

		ut_ad(!is_predefined_tablespace(i->first));
		space = i->second.space;
		for (j = 0; j < page_no_arr_len; j++)
		{
			uint32_t page_no = page_no_arr[j];

			recv_addr = pm_ppl_recv_get_fil_addr_struct(recv_line, space->id, page_no);

			if (recv_addr != NULL){
				printf("Apply page %zu for space %zu name %s\n", page_no, space->id, space->name);
				pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
			} else {
				printf("Page %zu of space %zu name %s is NOT in hashtable\n",page_no, space->id, space->name);
			}
		}

	}//end for

	printf("PMEM_INFO: END apply prior pages\n");
}

/*
 * Simulate recv_apply_hashed_log_recs()
 * for each line, get recv from hashtable and parse it
 * */
void
pm_ppl_recv_apply_hashed_log_recs(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		ibool	allow_ibuf)
{
	//bool is_multi_redo = false;	
	bool is_multi_redo = true;	
	uint32_t n, i;
	uint32_t begin;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;
    PMEM_LOG_REDOER*				redoer;
	
	
	//set this variable to a value > n lines to skip applying	
	begin = 0;

	n = ppl->n_buckets;

	if (IS_GLOBAL_HASHTABLE) {
		//test use global hashtable
		recv_line = ppl->recv_line;
		recv_line->is_ibuf_avail = allow_ibuf;
		printf("===> PMEM_RECV Phase 2: START Applying GLOBAL line n_cell %zu\n", recv_line->n_addrs);
		pm_ppl_recv_apply_hashed_line(pop, ppl, NULL, recv_line->is_ibuf_avail);

		return;
		//end test use global hashtable
	}
	
	if (!is_multi_redo){
		for (i = begin; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);
			recv_line = pline->recv_line;

			/*if a line has recv_diskaddr == ULONG_MAX, that line has all free page block => skip REDO*/
			if (recv_line->n_addrs == 0)
				continue;

			recv_line->is_ibuf_avail = allow_ibuf;

			//test, serialize for easy debug
			printf("===> PMEM_RECV Phase 2: START Applying line %zu n_cell %zu\n", pline->hashed_id, pline->recv_line->n_addrs);
			pm_ppl_recv_apply_hashed_line(pop, ppl, pline, pline->recv_line->is_ibuf_avail);

			printf("===>done Applying line %zu n_cell %zu\n", pline->hashed_id, pline->recv_line->n_addrs);
		}
		printf("\nPMEM_RECV: ===== Applying completed ======\n");
		return;
	}
	
	/* Parallelism APPLYING*/

    //(1) Init the redoers and create threads
    redoer = pm_log_redoer_init(n);
	
	//set type as APPLY
	redoer->phase = PMEM_REDO_PHASE2;

    ppl->redoer = redoer;

    redoer->n_remains = n;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		recv_line = pline->recv_line;
		
        if (recv_line->n_addrs == 0){
            redoer->n_remains--;
        }
    }

    printf("PMEM_RECV: Need applying %zu lines\n", redoer->n_remains);

    ppl->is_redoing_done = false;

    //create threads
    for (i = 0; i < srv_ppl_n_log_flush_threads; ++i) {
        os_thread_create(pm_log_redoer_worker, NULL, NULL);
    }
    
    // (2) Assign pointers
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		recv_line = pline->recv_line;
        
        //this variable is set true after a thread accept to REDO this line
        pline->is_redoing = false;

        /*if a line has recv_diskaddr == ULONG_MAX, that line has all free page block => skip REDO*/
        if (recv_line->n_addrs == 0)
            continue;
		
		recv_line->is_ibuf_avail = allow_ibuf;
        //Asign pline to a redoer thread
        redoer->hashed_line_arr[i] = pline;

    }
	

    //trigger REDOer threads
    os_event_set(redoer->is_log_req_not_empty);
    
    //(3) wait for all threads finish
    while (redoer->n_remains > 0){
        os_event_wait(redoer->is_log_all_finished);
    }

finish: 
    //(4) finish
    pm_log_redoer_close(ppl->redoer);
    printf("\nPMEM_RECV: ===== Applying completed ======\n");
}

/*
 * Check a hashed line after PARSE and before APPLY
 * */
void
pm_ppl_recv_check_hashed_line(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline) {

	recv_addr_t* recv_addr;
	ulint	i;

	PMEM_RECV_LINE* recv_line;
	recv_t*		recv;
	recv_t*		prev_recv;

	mtr_t	mtr;

	PMEM_PAGE_LOG_BLOCK* plog_block;
	ulint key;

	ulint pass_cnt;
	ulint zero_cnt;


	if (pline != NULL){ 
		recv_line	= pline->recv_line;
	} else {
		recv_line = ppl->recv_line;
	}
	
	pass_cnt = zero_cnt = 0;

	//for each cell in the hashtable
	for (i = 0; i < hash_get_n_cells(recv_line->addr_hash); i++) {
		//for each recv_addr

		for (recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_FIRST(recv_line->addr_hash, i));
		     recv_addr != 0;
		     recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_NEXT(addr_hash, recv_addr))) {

			if (srv_is_tablespace_truncated(recv_addr->space)) {
				continue;
			}
			if (recv_addr->state == RECV_DISCARDED) {
				continue;
			}
			
			ulint n_recvs = UT_LIST_GET_LEN(recv_addr->rec_list);

			//(1) Ensure the space is found
			const page_id_t		page_id(recv_addr->space,
							recv_addr->page_no);
			bool			found;
			const page_size_t&	page_size
				= fil_space_get_page_size(recv_addr->space,
							  &found);

			ut_ad(found);
			//(2) Check page in recv_addr is valid
			buf_block_t*	block;
			dberr_t			err;
			byte*			page;
			bool read_ok;

			mtr_start(&mtr);
			mtr_set_log_mode(&mtr, MTR_LOG_NONE);

			if (buf_page_peek(page_id)) {
				block = buf_page_get(
						page_id, page_size,
						RW_X_LATCH, &mtr);
				buf_block_dbg_add_level(
						block, SYNC_NO_ORDER_CHECK);
			} else {
				read_ok = buf_read_page(
						page_id, page_size);

				assert(read_ok);

				block = buf_page_get(
						page_id, page_size,
						RW_X_LATCH, &mtr);

			}
			assert(block != NULL);
			page = block->frame;

			ulint read_space_id = mach_read_from_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
			ulint read_page_no = mach_read_from_4(page + FIL_PAGE_OFFSET);

			if(read_space_id != block->page.id.space() || read_page_no != block->page.id.page_no()){
				printf("PMEM_ERROR in pm_ppl_recv_check_hashed_line(), input (space %zu, page_no %zu) differ read (space %zu, page_no %zu) n_recvs %zu\n", block->page.id.space(), block->page.id.page_no(), read_space_id, read_page_no, n_recvs);

				zero_cnt++;
			}
			else{
				pass_cnt++;
			}

			mtr.discard_modifications();

			mtr_commit(&mtr);
				

			//(3) Check log record ordering LSN1 < LSN2 if log rec 1 write before log rec 2
			recv = UT_LIST_GET_FIRST(recv_addr->rec_list);
			prev_recv = NULL;	

			while (recv) {
				if (prev_recv != NULL){
					assert(prev_recv->start_lsn <= recv->start_lsn);
				}

				prev_recv = recv;
				recv = UT_LIST_GET_NEXT(rec_list, recv);
			}//end while (recv)
			
		} //end for each recv_addr
	}//end for each cell in the hashtable

	printf("END TEST: pass_cnt %zu zero_cnt %zu total %zu \n", pass_cnt, zero_cnt, recv_line->n_addrs);
}
/*
 *allow_ibuf only is set FALSE in the begining of recv_group_scan_log_recs()
 * */
void
pm_ppl_recv_apply_hashed_line(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ibool	allow_ibuf)
{
	recv_addr_t* recv_addr;
	ulint	i;
	ulint	cnt, cnt2;
	ulint	n, n_cells;
	ibool	has_printed	= FALSE;
	mtr_t	mtr;

	
	PMEM_RECV_LINE* recv_line;
	if (pline != NULL){ 
		recv_line	= pline->recv_line;
	} else {
		recv_line = ppl->recv_line;
	}
	//if (recv_line->apply_batch_on) {
	//	os_thread_sleep(500000);
	//	goto loop;
	//}

	//ut_ad(!allow_ibuf == log_mutex_own());

	if (!allow_ibuf) {
		recv_no_ibuf_operations = true;
	}

	recv_line->apply_log_recs = TRUE;
	recv_line->apply_batch_on = TRUE;
	
	recv_line->skip_zero_page = true;

	cnt = 0;
	cnt2 = 10;

	n = recv_line->n_addrs;
		
	printf("Start apply log recs ... \n");
	n_cells = hash_get_n_cells(recv_line->addr_hash);

loop:

	for (i = 0; i < hash_get_n_cells(recv_line->addr_hash); i++) {
		for (recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_FIRST(recv_line->addr_hash, i));
		     recv_addr != 0;
		     recv_addr = static_cast<recv_addr_t*>(
				HASH_GET_NEXT(addr_hash, recv_addr))) {

			if (srv_is_tablespace_truncated(recv_addr->space)) {
				/* Avoid applying REDO log for the tablespace
				that is schedule for TRUNCATE. */
				ut_a(recv_line->n_addrs);
				recv_addr->state = RECV_DISCARDED;
				recv_line->n_addrs--;
				cnt++;
				continue;
			}

			if (recv_addr->state == RECV_DISCARDED) {
				ut_a(recv_line->n_addrs);
				recv_line->n_addrs--;
				cnt++;
				continue;
			}

			const page_id_t		page_id(recv_addr->space,
							recv_addr->page_no);
			bool			found;
			const page_size_t&	page_size
				= fil_space_get_page_size(recv_addr->space,
							  &found);

			ut_ad(found);

			if (recv_addr->state == RECV_NOT_PROCESSED) {
				if (buf_page_peek(page_id)) {
					buf_block_t*	block;

					mtr_start(&mtr);

					block = buf_page_get( page_id, page_size, RW_X_LATCH, &mtr);

					buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

					pm_ppl_recv_recover_page_func(pop, ppl, pline, FALSE, block);

					mtr_commit(&mtr);
				} else {
					// apply is done in IO thread after this read call is called

					pm_ppl_recv_read_in_area (recv_line, page_id);
					//bool read_ok = buf_read_page(page_id, page_size);

					//assert(read_ok);
				}
			}
			cnt++;
			
			if (IS_GLOBAL_HASHTABLE){
				if (cnt * 1.0 * 100 / n > cnt2){
					printf("Global HT, Apply cell %zu / %zu cnt %zu cnt2 %zu percentage processed \n", i, n_cells, cnt, cnt2);
					cnt2 += 10;
				}
			}
			//if (((cnt * 100 / n) > cnt2) ){
			//	printf("%zu ", cnt2);
			//	cnt2 += 10;
			//}
		}//end inner for

	}//end outer for 
	
	//if (recv_line->n_addrs > 0){
	//	recv_line->skip_zero_page = false;
	//	printf("=============\n HANDLE ZERO PAGES \n===============\n");
	//	goto loop;
	//}

	/* Wait until all the pages have been processed */
	while (recv_line->n_addrs != 0) {
		printf("recv_line %zu has n_addrs is %zu, wait until all the pages are done\n", recv_line->hashed_id, recv_line->n_addrs);
		os_thread_sleep(500000);
	}

	if (!allow_ibuf) {
		//TODO: we don't implement this now
		printf("====> PMEM_RECV: TODO allow_ibuf FALSE in pm_ppl_recv_apply_hashed_log_recs() line %zu\n", recv_line->hashed_id);	
		assert(0);
	}

	recv_line->apply_log_recs = FALSE;
	recv_line->apply_batch_on = FALSE;

	//simulate recv_sys_empty_hash()
	pm_ppl_recv_line_empty_hash(pop, ppl, pline);

	if (has_printed){
		fprintf(stderr, "Apply log for line %zu finished\n", recv_line->hashed_id);
	}

}


/*
 *Test the header of a log record (from ptr to end_ptr)
 * */
void
pm_ppl_check_input_rec(
		byte*		    ptr,
		byte*           end_ptr,
		mlog_id_t*      type,
	    ulint*          space,
		ulint*          page_no
		)
{
   byte* new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);

}
#endif //UNIV_PMEMOBJ_PART_PL

/** Start recovering from a redo log checkpoint.
@see recv_recovery_from_checkpoint_finish
@param[in]	flush_lsn	FIL_PAGE_FILE_FLUSH_LSN
of first system tablespace page
@return error code or DB_SUCCESS */
dberr_t
recv_recovery_from_checkpoint_start(
	lsn_t	flush_lsn)
{
	log_group_t*	group;
	log_group_t*	max_cp_group;
	ulint		max_cp_field;
	lsn_t		checkpoint_lsn;
	bool		rescan;
	ib_uint64_t	checkpoint_no;
	lsn_t		contiguous_lsn;
	byte*		buf;
	byte		log_hdr_buf[LOG_FILE_HDR_SIZE];
	dberr_t		err;

	/* Initialize red-black tree for fast insertions into the
	flush_list during recovery process. */
	buf_flush_init_flush_rbt();

	if (srv_force_recovery >= SRV_FORCE_NO_LOG_REDO) {

		ib::info() << "The user has set SRV_FORCE_NO_LOG_REDO on,"
			" skipping log redo";

		return(DB_SUCCESS);
	}

	recv_recovery_on = true;

	log_mutex_enter();

	/* Look for the latest checkpoint from any of the log groups */

	err = recv_find_max_checkpoint(&max_cp_group, &max_cp_field);

	if (err != DB_SUCCESS) {

		log_mutex_exit();

		return(err);
	}

	log_group_header_read(max_cp_group, max_cp_field);

	buf = log_sys->checkpoint_buf;

	checkpoint_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
	checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

	/* Read the first log file header to print a note if this is
	a recovery from a restored InnoDB Hot Backup */

	const page_id_t	page_id(max_cp_group->space_id, 0);

	fil_io(IORequestLogRead, true, page_id, univ_page_size, 0,
	       LOG_FILE_HDR_SIZE, log_hdr_buf, max_cp_group);

	if (0 == ut_memcmp(log_hdr_buf + LOG_HEADER_CREATOR,
			   (byte*)"ibbackup", (sizeof "ibbackup") - 1)) {

		if (srv_read_only_mode) {
			log_mutex_exit();

			ib::error() << "Cannot restore from mysqlbackup,"
				" InnoDB running in read-only mode!";

			return(DB_ERROR);
		}

		/* This log file was created by mysqlbackup --restore: print
		a note to the user about it */

		ib::info() << "The log file was created by mysqlbackup"
			" --apply-log at "
			<< log_hdr_buf + LOG_HEADER_CREATOR
			<< ". The following crash recovery is part of a"
			" normal restore.";

		/* Replace the label. */
		ut_ad(LOG_HEADER_CREATOR_END - LOG_HEADER_CREATOR
		      >= sizeof LOG_HEADER_CREATOR_CURRENT);
		memset(log_hdr_buf + LOG_HEADER_CREATOR, 0,
		       LOG_HEADER_CREATOR_END - LOG_HEADER_CREATOR);
		strcpy(reinterpret_cast<char*>(log_hdr_buf)
		       + LOG_HEADER_CREATOR, LOG_HEADER_CREATOR_CURRENT);

		/* Write to the log file to wipe over the label */
		fil_io(IORequestLogWrite, true, page_id,
		       univ_page_size, 0, OS_FILE_LOG_BLOCK_SIZE, log_hdr_buf,
		       max_cp_group);
	}

	/* Start reading the log groups from the checkpoint lsn up. The
	variable contiguous_lsn contains an lsn up to which the log is
	known to be contiguously written to all log groups. */

	recv_sys->mlog_checkpoint_lsn = 0;

	ut_ad(RECV_SCAN_SIZE <= log_sys->buf_size);

	ut_ad(UT_LIST_GET_LEN(log_sys->log_groups) == 1);
	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	ut_ad(recv_sys->n_addrs == 0);
	contiguous_lsn = checkpoint_lsn;
	switch (group->format) {
	case 0:
		log_mutex_exit();
		return(recv_log_format_0_recover(checkpoint_lsn));
	case LOG_HEADER_FORMAT_CURRENT:
		break;
	default:
		ut_ad(0);
		recv_sys->found_corrupt_log = true;
		log_mutex_exit();
		return(DB_ERROR);
	}

	/** Scan the redo log from checkpoint lsn and redo log to
	the hash table. */
	rescan = recv_group_scan_log_recs(group, &contiguous_lsn, false);


	if ((recv_sys->found_corrupt_log && !srv_force_recovery)
	    || recv_sys->found_corrupt_fs) {
		log_mutex_exit();
		return(DB_ERROR);
	}

	if (recv_sys->mlog_checkpoint_lsn == 0) {
		if (!srv_read_only_mode
		    && group->scanned_lsn != checkpoint_lsn) {
			ib::error() << "Ignoring the redo log due to missing"
				" MLOG_CHECKPOINT between the checkpoint "
				<< checkpoint_lsn << " and the end "
				<< group->scanned_lsn << ".";
			if (srv_force_recovery < SRV_FORCE_NO_LOG_REDO) {
				log_mutex_exit();
				return(DB_ERROR);
			}
		}

		group->scanned_lsn = checkpoint_lsn;
		rescan = false;
	}

	/* NOTE: we always do a 'recovery' at startup, but only if
	there is something wrong we will print a message to the
	user about recovery: */

	if (checkpoint_lsn != flush_lsn) {

		if (checkpoint_lsn + SIZE_OF_MLOG_CHECKPOINT < flush_lsn) {
			ib::warn() << " Are you sure you are using the"
				" right ib_logfiles to start up the database?"
				" Log sequence number in the ib_logfiles is "
				<< checkpoint_lsn << ", less than the"
				" log sequence number in the first system"
				" tablespace file header, " << flush_lsn << ".";
		}

		if (!recv_needed_recovery) {

			ib::info() << "The log sequence number " << flush_lsn
				<< " in the system tablespace does not match"
				" the log sequence number " << checkpoint_lsn
				<< " in the ib_logfiles!";

			if (srv_read_only_mode) {
				ib::error() << "Can't initiate database"
					" recovery, running in read-only-mode.";
				log_mutex_exit();
				return(DB_READ_ONLY);
			}

			recv_init_crash_recovery();
		}
	}

	log_sys->lsn = recv_sys->recovered_lsn;

	if (recv_needed_recovery) {
		err = recv_init_crash_recovery_spaces();

		if (err != DB_SUCCESS) {
			log_mutex_exit();
			return(err);
		}

		if (rescan) {
			//tdnguyen test
			printf("====> NEED RESCAN, contigous_lsn %zu\n", contiguous_lsn);
			contiguous_lsn = checkpoint_lsn;
			recv_group_scan_log_recs(group, &contiguous_lsn, true);

			if ((recv_sys->found_corrupt_log
			     && !srv_force_recovery)
			    || recv_sys->found_corrupt_fs) {
				log_mutex_exit();
				return(DB_ERROR);
			}
		}
	} else {
		ut_ad(!rescan || recv_sys->n_addrs == 0);
	}

	/* We currently have only one log group */

	if (group->scanned_lsn < checkpoint_lsn
	    || group->scanned_lsn < recv_max_page_lsn) {

		ib::error() << "We scanned the log up to " << group->scanned_lsn
			<< ". A checkpoint was at " << checkpoint_lsn << " and"
			" the maximum LSN on a database page was "
			<< recv_max_page_lsn << ". It is possible that the"
			" database is now corrupt!";
	}

	if (recv_sys->recovered_lsn < checkpoint_lsn) {
		log_mutex_exit();

		/* No harm in trying to do RO access. */
		if (!srv_read_only_mode) {
			ut_error;
		}

		return(DB_ERROR);
	}

	/* Synchronize the uncorrupted log groups to the most up-to-date log
	group; we also copy checkpoint info to groups */

	log_sys->next_checkpoint_lsn = checkpoint_lsn;
	log_sys->next_checkpoint_no = checkpoint_no + 1;

	recv_synchronize_groups();

	if (!recv_needed_recovery) {
		ut_a(checkpoint_lsn == recv_sys->recovered_lsn);
	} else {
		srv_start_lsn = recv_sys->recovered_lsn;
	}

	ut_memcpy(log_sys->buf, recv_sys->last_block, OS_FILE_LOG_BLOCK_SIZE);
	log_sys->buf_free = (ulint) log_sys->lsn % OS_FILE_LOG_BLOCK_SIZE;
	log_sys->buf_next_to_write = log_sys->buf_free;
	log_sys->write_lsn = log_sys->lsn;

	log_sys->last_checkpoint_lsn = checkpoint_lsn;

	if (!srv_read_only_mode) {
		/* Write a MLOG_CHECKPOINT marker as the first thing,
		before generating any other redo log. */
		fil_names_clear(log_sys->last_checkpoint_lsn, true);
	}

	MONITOR_SET(MONITOR_LSN_CHECKPOINT_AGE,
		    log_sys->lsn - log_sys->last_checkpoint_lsn);

	log_sys->next_checkpoint_no = checkpoint_no + 1;

#if defined(UNIV_PMEMOBJ_LOG) || defined (UNIV_PMEMOBJ_WAL)
	if (	gb_pmw->plogbuf->buf_free > log_sys->buf_free &&
			gb_pmw->plogbuf->lsn > log_sys->lsn ) {
		uint64_t len = gb_pmw->plogbuf->buf_free - log_sys->buf_free;
		uint64_t d_lsn = gb_pmw->plogbuf->lsn - log_sys->lsn;
		printf("[PMEMOBJ_INFO]!!!!!!!  Migration persistent log buffer to D-RAM log buffer and make the D-RAM log buffer persist\n");
		//If the pmem log buffer is newer than current log buffer, we have more work to do
		byte* p_old = (byte*) pm_wrapper_logbuf_get_logdata(gb_pmw);
		byte* p_new;

		size_t size = gb_pmw->plogbuf->size;

		ut_memcpy(log_sys->buf + log_sys->buf_free, p_old + log_sys->buf_free, len);

		log_sys->buf_free += len;
		log_sys->lsn += d_lsn;
		//reallocate the log buffer in PMEM
		pm_wrapper_logbuf_realloc(gb_pmw, size);
		p_new = (byte*) pm_wrapper_logbuf_get_logdata(gb_pmw); 
		//copy the log records from D-RAM heap
		pmemobj_memcpy_persist(gb_pmw->pop, p_new, log_sys->buf_ptr, log_sys->buf_free);
		//We free the heap from DRAM
		ut_free(log_sys->buf_ptr);

		log_sys->buf_ptr = static_cast<byte*> (pm_wrapper_logbuf_get_logdata(gb_pmw));
		log_sys->buf = static_cast<byte*>(
				ut_align(log_sys->buf_ptr, OS_FILE_LOG_BLOCK_SIZE));
		
		gb_pmw->plogbuf->lsn = log_sys->lsn;
		gb_pmw->plogbuf->buf_free = log_sys->buf_free;
	}

#endif

	mutex_enter(&recv_sys->mutex);

	recv_sys->apply_log_recs = TRUE;

	mutex_exit(&recv_sys->mutex);

	log_mutex_exit();

	recv_lsn_checks_on = true;

	/* The database is now ready to start almost normal processing of user
	transactions: transaction rollbacks and the application of the log
	records in the hash table can be run in background. */

	return(DB_SUCCESS);
}

/** Complete recovery from a checkpoint. */
void
recv_recovery_from_checkpoint_finish(void)
{
	/* Make sure that the recv_writer thread is done. This is
	required because it grabs various mutexes and we want to
	ensure that when we enable sync_order_checks there is no
	mutex currently held by any thread. */
	mutex_enter(&recv_sys->writer_mutex);

	/* Free the resources of the recovery system */
	recv_recovery_on = false;

	/* By acquring the mutex we ensure that the recv_writer thread
	won't trigger any more LRU batches. Now wait for currently
	in progress batches to finish. */
	buf_flush_wait_LRU_batch_end();

	mutex_exit(&recv_sys->writer_mutex);

	ulint count = 0;
	while (recv_writer_thread_active) {
		++count;
		os_thread_sleep(100000);
		if (srv_print_verbose_log && count > 600) {
			ib::info() << "Waiting for recv_writer to"
				" finish flushing of buffer pool";
			count = 0;
		}
	}

	recv_sys_debug_free();

	/* Free up the flush_rbt. */
	buf_flush_free_flush_rbt();

	/* Validate a few system page types that were left uninitialized
	by older versions of MySQL. */
	mtr_t		mtr;
	buf_block_t*	block;
	mtr.start();
	mtr.set_sys_modified();
	/* Bitmap page types will be reset in buf_dblwr_check_block()
	without redo logging. */
	block = buf_page_get(
		page_id_t(IBUF_SPACE_ID, FSP_IBUF_HEADER_PAGE_NO),
		univ_page_size, RW_X_LATCH, &mtr);
	fil_block_check_type(block, FIL_PAGE_TYPE_SYS, &mtr);
	/* Already MySQL 3.23.53 initialized FSP_IBUF_TREE_ROOT_PAGE_NO
	to FIL_PAGE_INDEX. No need to reset that one. */
	block = buf_page_get(
		page_id_t(TRX_SYS_SPACE, TRX_SYS_PAGE_NO),
		univ_page_size, RW_X_LATCH, &mtr);
	fil_block_check_type(block, FIL_PAGE_TYPE_TRX_SYS, &mtr);
	block = buf_page_get(
		page_id_t(TRX_SYS_SPACE, FSP_FIRST_RSEG_PAGE_NO),
		univ_page_size, RW_X_LATCH, &mtr);
	fil_block_check_type(block, FIL_PAGE_TYPE_SYS, &mtr);
	block = buf_page_get(
		page_id_t(TRX_SYS_SPACE, FSP_DICT_HDR_PAGE_NO),
		univ_page_size, RW_X_LATCH, &mtr);
	fil_block_check_type(block, FIL_PAGE_TYPE_SYS, &mtr);
	mtr.commit();

	/* Roll back any recovered data dictionary transactions, so
	that the data dictionary tables will be free of any locks.
	The data dictionary latch should guarantee that there is at
	most one data dictionary transaction active at a time. */
	if (srv_force_recovery < SRV_FORCE_NO_TRX_UNDO) {
		trx_rollback_or_clean_recovered(FALSE);
	}
}

/********************************************************//**
Initiates the rollback of active transactions. */
void
recv_recovery_rollback_active(void)
/*===============================*/
{
	ut_ad(!recv_writer_thread_active);

	/* Switch latching order checks on in sync0debug.cc, if
	--innodb-sync-debug=true (default) */
	ut_d(sync_check_enable());

	/* We can't start any (DDL) transactions if UNDO logging
	has been disabled, additionally disable ROLLBACK of recovered
	user transactions. */
	if (srv_force_recovery < SRV_FORCE_NO_TRX_UNDO
	    && !srv_read_only_mode) {

		/* Drop partially created indexes. */
		row_merge_drop_temp_indexes();
		/* Drop temporary tables. */
		row_mysql_drop_temp_tables();

		/* Drop any auxiliary tables that were not dropped when the
		parent table was dropped. This can happen if the parent table
		was dropped but the server crashed before the auxiliary tables
		were dropped. */
		fts_drop_orphaned_tables();

		/* Rollback the uncommitted transactions which have no user
		session */

		trx_rollback_or_clean_is_active = true;
		os_thread_create(trx_rollback_or_clean_all_recovered, 0, 0);
	}
}

/******************************************************//**
Resets the logs. The contents of log files will be lost! */
void
recv_reset_logs(
/*============*/
	lsn_t		lsn)		/*!< in: reset to this lsn
					rounded up to be divisible by
					OS_FILE_LOG_BLOCK_SIZE, after
					which we add
					LOG_BLOCK_HDR_SIZE */
{
	log_group_t*	group;

	ut_ad(log_mutex_own());

	log_sys->lsn = ut_uint64_align_up(lsn, OS_FILE_LOG_BLOCK_SIZE);

	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	while (group) {
		group->lsn = log_sys->lsn;
		group->lsn_offset = LOG_FILE_HDR_SIZE;
		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	log_sys->buf_next_to_write = 0;
	log_sys->write_lsn = log_sys->lsn;

	log_sys->next_checkpoint_no = 0;
	log_sys->last_checkpoint_lsn = 0;

	log_block_init(log_sys->buf, log_sys->lsn);
	log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

	log_sys->buf_free = LOG_BLOCK_HDR_SIZE;
	log_sys->lsn += LOG_BLOCK_HDR_SIZE;

	MONITOR_SET(MONITOR_LSN_CHECKPOINT_AGE,
		    (log_sys->lsn - log_sys->last_checkpoint_lsn));

	log_mutex_exit();

	/* Reset the checkpoint fields in logs */

	log_make_checkpoint_at(LSN_MAX, TRUE);

	log_mutex_enter();
}
#endif /* !UNIV_HOTBACKUP */

#ifdef UNIV_HOTBACKUP
/******************************************************//**
Creates new log files after a backup has been restored. */
void
recv_reset_log_files_for_backup(
/*============================*/
	const char*	log_dir,	/*!< in: log file directory path */
	ulint		n_log_files,	/*!< in: number of log files */
	lsn_t		log_file_size,	/*!< in: log file size */
	lsn_t		lsn)		/*!< in: new start lsn, must be
					divisible by OS_FILE_LOG_BLOCK_SIZE */
{
	os_file_t	log_file;
	bool		success;
	byte*		buf;
	ulint		i;
	ulint		log_dir_len;
	char		name[5000];
	static const char ib_logfile_basename[] = "ib_logfile";

	log_dir_len = strlen(log_dir);
	/* full path name of ib_logfile consists of log dir path + basename
	+ number. This must fit in the name buffer.
	*/
	ut_a(log_dir_len + strlen(ib_logfile_basename) + 11  < sizeof(name));

	buf = (byte*)ut_zalloc_nokey(LOG_FILE_HDR_SIZE +
				     OS_FILE_LOG_BLOCK_SIZE);

	for (i = 0; i < n_log_files; i++) {

		sprintf(name, "%s%s%lu", log_dir,
			ib_logfile_basename, (ulong) i);

		log_file = os_file_create_simple(innodb_log_file_key,
						 name, OS_FILE_CREATE,
						 OS_FILE_READ_WRITE,
						 srv_read_only_mode, &success);
		if (!success) {
			ib::fatal() << "Cannot create " << name << ". Check that"
				" the file does not exist yet.";
		}

		ib::info() << "Setting log file size to " << log_file_size;

		success = os_file_set_size(
			name, log_file, log_file_size, srv_read_only_mode);

		if (!success) {
			ib::fatal() << "Cannot set " << name << " size to "
				<< (long long unsigned)log_file_size;
		}

		os_file_flush(log_file);
		os_file_close(log_file);
	}

	/* We pretend there is a checkpoint at lsn + LOG_BLOCK_HDR_SIZE */

	log_reset_first_header_and_checkpoint(buf, lsn);

	log_block_init(buf + LOG_FILE_HDR_SIZE, lsn);
	log_block_set_first_rec_group(buf + LOG_FILE_HDR_SIZE,
				      LOG_BLOCK_HDR_SIZE);
	log_block_set_checksum(buf + LOG_FILE_HDR_SIZE,
	log_block_calc_checksum_crc32(buf + LOG_FILE_HDR_SIZE));

	log_block_set_checksum(buf, log_block_calc_checksum_crc32(buf));
	sprintf(name, "%s%s%lu", log_dir, ib_logfile_basename, (ulong)0);

	log_file = os_file_create_simple(innodb_log_file_key,
					 name, OS_FILE_OPEN,
					 OS_FILE_READ_WRITE,
					 srv_read_only_mode, &success);
	if (!success) {
		ib::fatal() << "Cannot open " << name << ".";
	}

	IORequest	request(IORequest::WRITE);

	dberr_t	err = os_file_write(
		request, name, log_file, buf, 0,
		LOG_FILE_HDR_SIZE + OS_FILE_LOG_BLOCK_SIZE);

	ut_a(err == DB_SUCCESS);

	os_file_flush(log_file);
	os_file_close(log_file);

	ut_free(buf);
}
#endif /* UNIV_HOTBACKUP */

/** Find a doublewrite copy of a page.
@param[in]	space_id	tablespace identifier
@param[in]	page_no		page number
@return	page frame
@retval NULL if no page was found */

const byte*
recv_dblwr_t::find_page(ulint space_id, ulint page_no)
{
	typedef std::vector<const byte*, ut_allocator<const byte*> >
		matches_t;

	matches_t	matches;
	const byte*	result = 0;

	for (list::iterator i = pages.begin(); i != pages.end(); ++i) {
		if (page_get_space_id(*i) == space_id
		    && page_get_page_no(*i) == page_no) {
			matches.push_back(*i);
		}
	}

	if (matches.size() == 1) {
		result = matches[0];
	} else if (matches.size() > 1) {

		lsn_t max_lsn	= 0;
		lsn_t page_lsn	= 0;

		for (matches_t::iterator i = matches.begin();
		     i != matches.end();
		     ++i) {

			page_lsn = mach_read_from_8(*i + FIL_PAGE_LSN);

			if (page_lsn > max_lsn) {
				max_lsn = page_lsn;
				result = *i;
			}
		}
	}

	return(result);
}

#ifndef DBUG_OFF
/** Return string name of the redo log record type.
@param[in]	type	record log record enum
@return string name of record log record */
const char*
get_mlog_string(mlog_id_t type)
{
	switch (type) {
	case MLOG_SINGLE_REC_FLAG:
		return("MLOG_SINGLE_REC_FLAG");

	case MLOG_1BYTE:
		return("MLOG_1BYTE");

	case MLOG_2BYTES:
		return("MLOG_2BYTES");

	case MLOG_4BYTES:
		return("MLOG_4BYTES");

	case MLOG_8BYTES:
		return("MLOG_8BYTES");

	case MLOG_REC_INSERT:
		return("MLOG_REC_INSERT");

	case MLOG_REC_CLUST_DELETE_MARK:
		return("MLOG_REC_CLUST_DELETE_MARK");

	case MLOG_REC_SEC_DELETE_MARK:
		return("MLOG_REC_SEC_DELETE_MARK");

	case MLOG_REC_UPDATE_IN_PLACE:
		return("MLOG_REC_UPDATE_IN_PLACE");

	case MLOG_REC_DELETE:
		return("MLOG_REC_DELETE");

	case MLOG_LIST_END_DELETE:
		return("MLOG_LIST_END_DELETE");

	case MLOG_LIST_START_DELETE:
		return("MLOG_LIST_START_DELETE");

	case MLOG_LIST_END_COPY_CREATED:
		return("MLOG_LIST_END_COPY_CREATED");

	case MLOG_PAGE_REORGANIZE:
		return("MLOG_PAGE_REORGANIZE");

	case MLOG_PAGE_CREATE:
		return("MLOG_PAGE_CREATE");

	case MLOG_UNDO_INSERT:
		return("MLOG_UNDO_INSERT");

	case MLOG_UNDO_ERASE_END:
		return("MLOG_UNDO_ERASE_END");

	case MLOG_UNDO_INIT:
		return("MLOG_UNDO_INIT");

	case MLOG_UNDO_HDR_DISCARD:
		return("MLOG_UNDO_HDR_DISCARD");

	case MLOG_UNDO_HDR_REUSE:
		return("MLOG_UNDO_HDR_REUSE");

	case MLOG_UNDO_HDR_CREATE:
		return("MLOG_UNDO_HDR_CREATE");

	case MLOG_REC_MIN_MARK:
		return("MLOG_REC_MIN_MARK");

	case MLOG_IBUF_BITMAP_INIT:
		return("MLOG_IBUF_BITMAP_INIT");

#ifdef UNIV_LOG_LSN_DEBUG
	case MLOG_LSN:
		return("MLOG_LSN");
#endif /* UNIV_LOG_LSN_DEBUG */

	case MLOG_INIT_FILE_PAGE:
		return("MLOG_INIT_FILE_PAGE");

	case MLOG_WRITE_STRING:
		return("MLOG_WRITE_STRING");

	case MLOG_MULTI_REC_END:
		return("MLOG_MULTI_REC_END");

	case MLOG_DUMMY_RECORD:
		return("MLOG_DUMMY_RECORD");

	case MLOG_FILE_DELETE:
		return("MLOG_FILE_DELETE");

	case MLOG_COMP_REC_MIN_MARK:
		return("MLOG_COMP_REC_MIN_MARK");

	case MLOG_COMP_PAGE_CREATE:
		return("MLOG_COMP_PAGE_CREATE");

	case MLOG_COMP_REC_INSERT:
		return("MLOG_COMP_REC_INSERT");

	case MLOG_COMP_REC_CLUST_DELETE_MARK:
		return("MLOG_COMP_REC_CLUST_DELETE_MARK");

	case MLOG_COMP_REC_SEC_DELETE_MARK:
		return("MLOG_COMP_REC_SEC_DELETE_MARK");

	case MLOG_COMP_REC_UPDATE_IN_PLACE:
		return("MLOG_COMP_REC_UPDATE_IN_PLACE");

	case MLOG_COMP_REC_DELETE:
		return("MLOG_COMP_REC_DELETE");

	case MLOG_COMP_LIST_END_DELETE:
		return("MLOG_COMP_LIST_END_DELETE");

	case MLOG_COMP_LIST_START_DELETE:
		return("MLOG_COMP_LIST_START_DELETE");

	case MLOG_COMP_LIST_END_COPY_CREATED:
		return("MLOG_COMP_LIST_END_COPY_CREATED");

	case MLOG_COMP_PAGE_REORGANIZE:
		return("MLOG_COMP_PAGE_REORGANIZE");

	case MLOG_FILE_CREATE2:
		return("MLOG_FILE_CREATE2");

	case MLOG_ZIP_WRITE_NODE_PTR:
		return("MLOG_ZIP_WRITE_NODE_PTR");

	case MLOG_ZIP_WRITE_BLOB_PTR:
		return("MLOG_ZIP_WRITE_BLOB_PTR");

	case MLOG_ZIP_WRITE_HEADER:
		return("MLOG_ZIP_WRITE_HEADER");

	case MLOG_ZIP_PAGE_COMPRESS:
		return("MLOG_ZIP_PAGE_COMPRESS");

	case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
		return("MLOG_ZIP_PAGE_COMPRESS_NO_DATA");

	case MLOG_ZIP_PAGE_REORGANIZE:
		return("MLOG_ZIP_PAGE_REORGANIZE");

	case MLOG_FILE_RENAME2:
		return("MLOG_FILE_RENAME2");

	case MLOG_FILE_NAME:
		return("MLOG_FILE_NAME");

	case MLOG_CHECKPOINT:
		return("MLOG_CHECKPOINT");

	case MLOG_PAGE_CREATE_RTREE:
		return("MLOG_PAGE_CREATE_RTREE");

	case MLOG_COMP_PAGE_CREATE_RTREE:
		return("MLOG_COMP_PAGE_CREATE_RTREE");

	case MLOG_INIT_FILE_PAGE2:
		return("MLOG_INIT_FILE_PAGE2");

	case MLOG_INDEX_LOAD:
		return("MLOG_INDEX_LOAD");

	case MLOG_TRUNCATE:
		return("MLOG_TRUNCATE");
	}
	DBUG_ASSERT(0);
	return(NULL);
}
#endif /* !DBUG_OFF */
