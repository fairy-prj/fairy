/**********************************************************************

  p-xgroup-by.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"
#include "xthread.h"

#include "fairy.h"

static ID id_new;
static ID id_init_key_proc;
static ID id_yield;
static ID id_each;
static ID id_push;
static ID id_add_exports;
static ID id_start;
static ID id_set_njob_id;
static ID id_add_key;

static ID id_sort_by_bang;
static ID id_open_buffer;
static ID id_io;
static ID id_close;

static VALUE rb_cFairyPGroupBy;
static VALUE PPostFilter;
static VALUE MergeSortBuffer;
static VALUE MSBMerger;
static VALUE MSBCachedBuffer;

VALUE rb_cFairyPXGroupBy;
VALUE rb_cFairyPXGPostfilter;
VALUE rb_cFairyPXGDirctMergeSortBuffer;
VALUE rb_cFairyPXGDirctMergeSortBufferCachedBuffer;

typedef struct rb_fairy_p_xgroup_by_struct
{
  VALUE bjob;
  VALUE input;
  VALUE opts;
  VALUE id;
  long mod;
  
  VALUE postqueuing_policy;
  VALUE exports_queue;
  VALUE key_proc;
  
  VALUE *exports;
  long *counter;
} fairy_p_xgroup_by_t;

#define GetFairyPXGroupByPtr(obj, tobj) \
  TypedData_Get_Struct((obj), fairy_p_xgroup_by_t, &fairy_p_xgroup_by_data_type, (tobj))

static void
fairy_p_xgroup_by_mark(void *ptr)
{
  fairy_p_xgroup_by_t *gb = (fairy_p_xgroup_by_t*)ptr;
  int i;
  
  rb_gc_mark(gb->bjob);
  rb_gc_mark(gb->input);
  rb_gc_mark(gb->opts);
  rb_gc_mark(gb->id);

  rb_gc_mark(gb->postqueuing_policy);
  rb_gc_mark(gb->exports_queue);
  rb_gc_mark(gb->key_proc);
  
  for (i = 0; i < gb->mod; i++) {
    rb_gc_mark(gb->exports[i]);
  }
}

static void
fairy_p_xgroup_by_free(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
fairy_p_xgroup_by_memsize(const void *ptr)
{
  fairy_p_xgroup_by_t *gb = (fairy_p_xgroup_by_t*)ptr;

  return ptr ? sizeof(fairy_p_xgroup_by_t) +(sizeof(VALUE) + sizeof(long)) * gb->mod : 0;
}

static const rb_data_type_t fairy_p_xgroup_by_data_type = {
    "fairy_p_xgroup_by",
    {fairy_p_xgroup_by_mark, fairy_p_xgroup_by_free, fairy_p_xgroup_by_memsize,},
};


static VALUE
fairy_p_xgroup_by_alloc(VALUE klass)
{
  VALUE volatile obj;
  fairy_p_xgroup_by_t *gb;

  obj = TypedData_Make_Struct(klass, fairy_p_xgroup_by_t, &fairy_p_xgroup_by_data_type, gb);

  gb->bjob = Qnil;
  gb->input = Qnil;
  gb->opts = Qnil;
  gb->id = Qnil;
  
  gb->mod = 0;
  
  gb->postqueuing_policy = Qnil;
  gb->exports_queue = Qnil;
  gb->key_proc = Qnil;
  
  gb->exports = NULL;
  gb->counter = NULL;

  return obj;
}

static VALUE
rb_fairy_p_xgroup_by_initialize(VALUE self, VALUE id, VALUE ntask, VALUE bjob, VALUE opts, VALUE block_source)
{
  fairy_p_xgroup_by_t *gb;
  VALUE argv[] = {
    id,
    ntask,
    bjob,
    opts,
    block_source,
  };

  GetFairyPXGroupByPtr(self, gb);
  rb_call_super(5, argv);

  gb->bjob = rb_iv_get(self, "@bjob");
  gb->opts = rb_iv_get(self, "@opts");
  gb->id = rb_iv_get(self, "@id");
  
  gb->postqueuing_policy = rb_fairy_conf(NULL, gb->opts, "postqueuing_policy");
  gb->mod = NUM2LONG(rb_fairy_conf("GROUP_BY_NO_SEGMENT", gb->opts, "no_segment"));
  gb->exports_queue = rb_iv_get(self, "@exports_queue");

  gb->exports = ALLOC_N(VALUE, gb->mod);
  {
    long i;
    for (i = 0; i < gb->mod; i++) {
      gb->exports[i] = Qnil;
    }
  }
  
  gb->counter = ALLOC_N(long, gb->mod);
  {
    long i;
    for (i = 0; i < gb->mod; i++) {
      gb->counter[i] = 0;
    }
  }

  return self;
}

static VALUE
rb_fairy_p_xgroup_by_add_export(VALUE self, long key, VALUE export)
{
  fairy_p_xgroup_by_t *gb;
  GetFairyPXGroupByPtr(self, gb);

  gb->exports[key] = export;
  rb_funcall(gb->bjob, id_add_exports, 3, LONG2NUM(key), export, self);
  return self;
}

static VALUE start_block(VALUE, VALUE, int, VALUE*);
static VALUE start_main(VALUE);
static VALUE start_main_i(VALUE, VALUE, int, VALUE*);

static VALUE
rb_fairy_p_xgroup_by_start_export(VALUE self)
{
  fairy_p_xgroup_by_t *gb;
  GetFairyPXGroupByPtr(self, gb);
  
  rb_fairy_debug(self, "START_EXPORT");

  gb->key_proc = rb_funcall(self, id_init_key_proc, 0);

  return rb_block_call(self, id_start, 0, 0, start_block, self);
}
  
static VALUE
start_block(VALUE e, VALUE self, int argc, VALUE *argv)
{
  VALUE result;
  fairy_p_xgroup_by_t *gb;
  int state;
  
  GetFairyPXGroupByPtr(self, gb);

  gb->input = rb_iv_get(self, "@input");
  
  result = rb_protect(start_main, self, &state);
  {
    long i;
    rb_xthread_queue_push(gb->exports_queue, Qnil);
    for (i = 0; i < gb->mod; i++) {
      static char buf[256];

      snprintf(buf, sizeof(buf), "G0 %d => %d", i, gb->counter[i]);
      rb_fairy_debug(self, buf);
      
      if (!NIL_P(gb->exports[i])) {
	rb_funcall(gb->exports[i], id_push, 1, rb_FairyEOS);
      }
    }
  
    if (state) {
      rb_fairy_debug_exception(self);
      rb_jump_tag(state);
    }
  }
  return result;
}

static VALUE
start_main(VALUE self)
{
  fairy_p_xgroup_by_t *gb;
  
  GetFairyPXGroupByPtr(self, gb);
  return rb_block_call(gb->input, id_each, 0, 0, start_main_i, self);
  
}

static VALUE
start_main_i(VALUE e, VALUE self, int argc, VALUE *argv)
{
  fairy_p_xgroup_by_t *gb;
  VALUE key;
  unsigned int hashkey;
  VALUE export;

  GetFairyPXGroupByPtr(self, gb);
  if (CLASS_OF(gb->key_proc) == rb_cProc) {
    key = rb_proc_call(gb->key_proc, rb_ary_new3(1, e));
  }
  else {
    key = rb_funcall(gb->key_proc, id_yield, 1, e);
  }
 
  if (CLASS_OF(key) == rb_cFairyImportCTLTOKEN_NULLVALUE) {
    return self;
  }

  hashkey = rb_fairy_simple_hash_uint(rb_mFairySimpleHash, key) % gb->mod;
  export = gb->exports[hashkey];
  if (NIL_P(export)) {
    export = rb_class_new_instance(1, &gb->postqueuing_policy, rb_cFairyExport);
    rb_funcall(export, id_set_njob_id, 1, gb->id);
    rb_funcall(export, id_add_key, 1, INT2FIX(hashkey));
    rb_fairy_p_xgroup_by_add_export(self, hashkey, export);
  }
  rb_funcall(export, id_push, 1, e);
  gb->counter[hashkey]++;
  return self;
}

#define xpf(name) fairy_pxg_postfilter##name
#define rb_xpf(name) rb_fairy_pxg_postfilter##name

typedef struct rb_xpf(_struct)
{
  VALUE buffering_policy;
  VALUE buffering_class;
  VALUE key_proc;
  VALUE key_value_buffer;
} xpf(_t);

#define GetFairyPXGPostFilterPtr(obj, tobj) \
  TypedData_Get_Struct((obj), xpf(_t), &xpf(_data_type), (tobj))

#define GetFXPFPtr(obj, tobj) \
  GetFairyPXGPostFilterPtr(obj, tobj)


static void
xpf(_mark)(void *ptr)
{
  xpf(_t) *pf = (xpf(_t)*)ptr;
  rb_gc_mark(pf->buffering_policy);
  rb_gc_mark(pf->buffering_class);
  rb_gc_mark(pf->key_proc);
  rb_gc_mark(pf->key_value_buffer);
}

static void
xpf(_free)(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
xpf(_memsize)(const void *ptr)
{
  xpf(_t) *pf = (xpf(_t)*)ptr;

  return ptr ? sizeof(xpf(_t)) : 0;
}

static const rb_data_type_t xpf(_data_type) = {
    "fairy_p_xgroup_by_postfiter",
    {xpf(_mark), xpf(_free), xpf(_memsize),},
};

static VALUE
xpf(_alloc)(VALUE klass)
{
  VALUE volatile obj;
  xpf(_t) *pf;

  obj = TypedData_Make_Struct(klass, xpf(_t), &xpf(_data_type), pf);

  pf->buffering_policy = Qnil;
  pf->buffering_class = Qnil;
  
  pf->key_proc = Qnil;
  pf->key_value_buffer = Qnil;

  return obj;
}

static VALUE
rb_xpf(_initialize)(VALUE self, VALUE id, VALUE ntask, VALUE bjob, VALUE opts, VALUE block_source)
{
  xpf(_t) *pf;
  VALUE klass_name;

  VALUE argv[] = {
    id, ntask,  bjob, opts, block_source,
  };
  GetFXPFPtr(self, pf);
  rb_call_super(5, argv);
  pf->buffering_policy = rb_iv_get(self, "@buffering_policy");

  klass_name = rb_funcall(pf->buffering_policy, rb_intern("[]"), 1,
			  ID2SYM(rb_intern("buffering_class")));
  if (NIL_P(klass_name)) {
    klass_name = rb_hash_aref(rb_fairy_conf("GROUP_BY_BUFFERING_POLICY", Qnil, NULL), ID2SYM(rb_intern("buffering_class")));
  }
   
 rb_fairy_debug_p(klass_name);
 pf->buffering_class = rb_const_get(rb_cFairyPXGroupBy, SYM2ID(klass_name));
 
  return self;
}

static VALUE
rb_xpf(_basic_each_input)(VALUE e, VALUE self, int argc, VALUE *argv)
{
  xpf(_t) *pf;
  GetFXPFPtr(self, pf);
  rb_funcall(pf->key_value_buffer, id_push, 1, e);
  return self;
}

static VALUE
rb_xpf(_basic_each_kvb)(VALUE kvs, VALUE self, int argc, VALUE *argv)
{
  rb_yield(kvs);
}

static VALUE
rb_xpf(_basic_each)(VALUE self)
{
  xpf(_t) *pf;
  VALUE input;
  VALUE arg[2];
  
  GetFXPFPtr(self, pf);

 arg[0] = self;
 arg[1] = pf->buffering_policy;
 
 pf->key_value_buffer = rb_class_new_instance(2, arg, pf->buffering_class);
 pf->key_proc = rb_funcall(self, id_init_key_proc, 0);

 input = rb_iv_get(self, "@input");
  
 rb_block_call(input, id_each, 0, 0, rb_xpf(_basic_each_input), self);
 rb_block_call(pf->key_value_buffer, id_each, 0, 0, rb_xpf(_basic_each_kvb), self);
 pf->key_value_buffer = Qnil;
 return self;
}


#define xmsb(name) fairy_pxg_direct_merge_sort_buffer##name
#define rb_xmsb(name) rb_fairy_pxg_direct_merge_sort_buffer##name

typedef struct rb_xmsb(_struct)
{
  VALUE njob;
  VALUE buffers;
  
  VALUE threshold;
  long CHUNK_SIZE;

  VALUE hash_proc;
  
} xmsb(_t);

#define GetFairyPXGDirectMergeSortBufferPtr(obj, tobj) \
  TypedData_Get_Struct((obj), xmsb(_t), &xmsb(_data_type), (tobj))

#define GetFXMSBPtr(obj, tobj) \
  GetFairyPXGDirectMergeSortBufferPtr(obj, tobj)

static void
xmsb(_mark)(void *ptr)
{
  xmsb(_t) *db = (xmsb(_t)*)ptr;
  rb_gc_mark(db->njob);
  rb_gc_mark(db->buffers);
  rb_gc_mark(db->threshold);
  rb_gc_mark(db->hash_proc);
}

static void
xmsb(_free)(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
xmsb(_memsize)(const void *ptr)
{
  xmsb(_t) *gb = (xmsb(_t)*)ptr;

  return ptr ? sizeof(xmsb(_t)) : 0;
}

static const rb_data_type_t xmsb(_data_type) = {
    "fairy_p_xgroup_by_direct_merge_sort_buffer",
    {xmsb(_mark), xmsb(_free), xmsb(_memsize),},
};

static VALUE
xmsb(_alloc)(VALUE klass)
{
  VALUE volatile obj;
  xmsb(_t) *db;

  obj = TypedData_Make_Struct(klass, xmsb(_t), &xmsb(_data_type), db);

  db->njob = Qnil;
  db->buffers = Qnil;
  db->threshold = Qnil;
  db->CHUNK_SIZE = 0;

  db->hash_proc = Qnil;
  
  return obj;
}

static VALUE
rb_xmsb(_initialize)(VALUE self, VALUE njob, VALUE policy)
{
  xmsb(_t) *db;

  VALUE argv[] = {
    njob,
    policy,
  };

  GetFXMSBPtr(self, db);
  rb_call_super(2, argv);

  db->njob = rb_iv_get(self, "@njob");
  db->threshold = rb_iv_get(self, "@threshold");
  db->CHUNK_SIZE = NUM2LONG(rb_iv_get(self, "@CHUNK_SIZE"));

  return self;
}

static VALUE
rb_xmsb(_init_2ndmemory)(VALUE self)
{
  xmsb(_t) *db;
  
  GetFXMSBPtr(self, db);

  rb_call_super(0, NULL);
  db->buffers = rb_iv_get(self, "@buffers");
}

static VALUE rb_xmsb(_store_2ndmemory_sort_b)(VALUE, VALUE, int, VALUE *);
static VALUE rb_xmsb(_store_2ndmemory_str)(VALUE, VALUE, VALUE, long *);
static VALUE rb_xmsb(_store_2ndmemory_obj)(VALUE, VALUE, VALUE, long *);

VALUE
rb_xmsb(_store_2ndmemory)(VALUE self, VALUE key_values)
{
  VALUE buffer = Qnil;
  VALUE io;
  long i;
  VALUE e;
  VALUE element_class = Qnil;
  xmsb(_t) *db;

  GetFXMSBPtr(self, db);
  
  rb_fairy_debug(self, "START STORE");

  if (NIL_P(db->hash_proc)) {
    db->hash_proc = rb_iv_get(db->njob, "@hash_proc");
  }

  rb_block_call(key_values, id_sort_by_bang, 0, 0,
		rb_xmsb(_store_2ndmemory_sort_b), self);

  buffer = rb_funcall(self, id_open_buffer, 0);
  io = rb_funcall(buffer, id_io, 0);
  i = 0;
  while (i < RARRAY_LEN(key_values)) {
    e = RARRAY_PTR(key_values)[i];
    if (CLASS_OF(e) == rb_cString) {
      rb_xmsb(_store_2ndmemory_str)(self, io, key_values, &i);
    }
    else {
      rb_xmsb(_store_2ndmemory_obj)(self, io, key_values, &i);
    }
  }
  rb_funcall(buffer, id_close, 0);
  
  rb_fairy_debug(self, "FINISH STORE");
  return self;
}

static VALUE
rb_xmsb(_store_2ndmemory_sort_b)(VALUE e, VALUE self, int argc, VALUE *argv)
{
  xmsb(_t) *db;
  VALUE key;

  GetFXMSBPtr(self, db);
  if (CLASS_OF(db->hash_proc) == rb_cProc) {
    key = rb_proc_call(db->hash_proc, rb_ary_new3(1, e));
  }
  else {
    key = rb_funcall(db->hash_proc, id_yield, 1, e);
  }
  return key;
}


static VALUE
rb_xmsb(_store_2ndmemory_str)(VALUE self, VALUE io, VALUE key_values, long *i)
{
  xmsb(_t) *db;
  VALUE sb;
  long start = *i;
  long j;
  VALUE e;
  
  GetFXMSBPtr(self, db);
  
  sb = rb_fairy_string_buffer_new();

  for (j = start;
       j < RARRAY_LEN(key_values) && j - start <= db->CHUNK_SIZE;
       j++) {
    e = RARRAY_PTR(key_values)[j];
    if (CLASS_OF(e) != rb_cString) {
      break;
    }
    rb_fairy_string_buffer_push(sb, e);
  }
  *i = j;
  rb_marshal_dump(sb, io);

  return self;
}

static VALUE
rb_xmsb(_store_2ndmemory_obj)(VALUE self, VALUE io, VALUE key_values, long *i)
{
  
  xmsb(_t) *db;
  VALUE sb;
  long start = *i;
  long j = start;
  VALUE e;
  
  GetFXMSBPtr(self, db);
  
  sb = rb_fairy_string_buffer_new();

  for (j = start;
       j < RARRAY_LEN(key_values) && j - start <= db->CHUNK_SIZE;
       j++) {
    e = RARRAY_PTR(key_values)[j];
    if (CLASS_OF(e) == rb_cString) {
      break;
    }
  }
  *i = j;

  if (start == 0 && j == RARRAY_LEN(key_values) -1) {
    rb_marshal_dump(key_values, io);
  }
  else {
    rb_marshal_dump(rb_ary_subseq(key_values, start, j - start), io);
  }

  return self;
}

static VALUE
rb_xmsb(_each_2ndmemory_sub)(VALUE values, VALUE self, int argc, VALUE *argv)
{
  return rb_yield(values);
}


static VALUE
rb_xmsb(_each_2ndmemory)(VALUE self)
{
  xmsb(_t) *db;
  VALUE key_values = rb_iv_get(self, "@key_values");
  VALUE m;

  GetFXMSBPtr(self, db);

  if (RARRAY_LEN(key_values) > 0) {
    rb_xmsb(_store_2ndmemory)(self, key_values);
    rb_iv_set(self, "@key_values", Qnil);
  }
  {
    VALUE arg[] = {
      db->njob,
      db->buffers,
      rb_cFairyPXGDirctMergeSortBufferCachedBuffer,
    };
   m = rb_class_new_instance(3, arg, MSBMerger);
   return rb_block_call(m, id_each, 0, 0, rb_xmsb(_each_2ndmemory_sub), self);
  }
}

#define xmsbcb(name) fairy_pxg_direct_merge_sort_buffer_cached_buffer##name
#define rb_xmsbcb(name) rb_fairy_pxg_direct_merge_sort_buffer_cached_buffer##name

typedef struct rb_xmsbcb(_struct)
{
  VALUE tmpbuf;
  VALUE io;
} xmsbcb(_t);

#define GetFairyPXGDirectMergeSortBufferCachedBufferPtr(obj, tobj) \
  TypedData_Get_Struct((obj), xmsbcb(_t), &xmsbcb(_data_type), (tobj))

#define GetFXMSBCBPtr(obj, tobj) \
  GetFairyPXGDirectMergeSortBufferCachedBufferPtr(obj, tobj)

static void
xmsbcb(_mark)(void *ptr)
{
  xmsbcb(_t) *cb = (xmsbcb(_t)*)ptr;
  rb_gc_mark(cb->tmpbuf);
  rb_gc_mark(cb->io);
}

static void
xmsbcb(_free)(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
xmsbcb(_memsize)(const void *ptr)
{
  xmsbcb(_t) *cb = (xmsbcb(_t)*)ptr;

  return ptr ? sizeof(xmsbcb(_t)) : 0;
}

static const rb_data_type_t xmsbcb(_data_type) = {
    "fairy_p_xgroup_by_direct_merge_sort_buffer_cached_buffer",
    {xmsbcb(_mark), xmsbcb(_free), xmsbcb(_memsize),},
};

static VALUE
xmsbcb(_alloc)(VALUE klass)
{
  VALUE volatile obj;
  xmsbcb(_t) *cb;

  obj = TypedData_Make_Struct(klass, xmsbcb(_t), &xmsbcb(_data_type), cb);

  cb->tmpbuf = Qnil;
  cb->io = Qnil;

  return obj;
}

static VALUE
rb_xmsbcb(_initialize)(VALUE self, VALUE njob, VALUE io)
{
  xmsbcb(_t) *cb;
  VALUE tmpbuf;

  VALUE argv[] = {
    njob,
    io,
  };

  GetFXMSBCBPtr(self, cb);
  cb->tmpbuf = io;
  rb_call_super(2, argv);
  
  return self;
}

/*
 * copy from eval_intern.h
 */

enum ruby_tag_type {
    RUBY_TAG_RETURN	= 0x1,
    RUBY_TAG_BREAK	= 0x2,
    RUBY_TAG_NEXT	= 0x3,
    RUBY_TAG_RETRY	= 0x4,
    RUBY_TAG_REDO	= 0x5,
    RUBY_TAG_RAISE	= 0x6,
    RUBY_TAG_THROW	= 0x7,
    RUBY_TAG_FATAL	= 0x8,
    RUBY_TAG_MASK	= 0xf
};
#define TAG_RETURN	RUBY_TAG_RETURN
#define TAG_BREAK	RUBY_TAG_BREAK
#define TAG_NEXT	RUBY_TAG_NEXT
#define TAG_RETRY	RUBY_TAG_RETRY
#define TAG_REDO	RUBY_TAG_REDO
#define TAG_RAISE	RUBY_TAG_RAISE
#define TAG_THROW	RUBY_TAG_THROW
#define TAG_FATAL	RUBY_TAG_FATAL
#define TAG_MASK	RUBY_TAG_MASK


static VALUE rb_xmsbcb(_read_buffer_sub)(VALUE);

#define DEBUG_MSG(msg) "rb_xmsbcb(_readbuffer): " #msg

VALUE
rb_xmsbcb(_read_buffer)(VALUE self)
{
  VALUE result;
  int state;
  xmsbcb(_t) *cb;

  GetFXMSBCBPtr(self, cb);
  
  rb_iv_set(self, "@cache_pv", INT2FIX(0));

  if (NIL_P(cb->io)) {
    cb->io = rb_funcall(cb->tmpbuf, id_io, 0);
  }
  
  if (RTEST(rb_io_eof(cb->io))) {
    rb_fairy_debugf(self, DEBUG_MSG(EOF reached: %s), RSTRING_PTR(rb_inspect(cb->io)));
    rb_iv_set(self, "@eof", Qtrue);
    rb_iv_set(self, "@cache", rb_ary_new());
    return self;
  }
  result = rb_protect(rb_xmsbcb(_read_buffer_sub), self, &state);
  if (state) {
    rb_fairy_debug(self, DEBUG_MSG(1 - rb_protext return non zero state!!));
    rb_fairy_debugf(self, DEBUG_MSG(state: %d), state);

    if (state == TAG_RAISE) {
      VALUE exp = rb_errinfo();
      
      rb_fairy_debug(self, DEBUG_MSG(2));  
      rb_fairy_debugf(self, DEBUG_MSG(Exeption: %s), RSTRING_PTR(rb_inspect(exp)));
      if (CLASS_OF(exp) ==  rb_eEOFError) {
	rb_fairy_debug(self, DEBUG_MSG(3 - EOF reached));
	rb_iv_set(self, "@eof", Qtrue);
	rb_iv_set(self, "@cache", rb_ary_new());
      }
      else if (CLASS_OF(exp) == rb_eArgError) {
	const char *head = "File Contents: ";
	/*	VALUE SEEK_CUR = rb_const_get(rb_cIO, rb_intern("SEEK_CUR")); */
	char *buf = ALLOCA_N(char, strlen(head) + 2048 + 1);
	VALUE readed;
	VALUE io;
	
	rb_fairy_debug(self, DEBUG_MSG(4 - MARSHAL ERROR OCCURED!!));  

	io = rb_funcall(cb->tmpbuf, id_io, 0);
	rb_funcall(io, rb_intern("seek"), 2, INT2NUM(-1024), SEEK_CUR);
	readed = rb_funcall(io, rb_intern("read"), 1, INT2NUM(2048));

	strcat(buf, head);
	strncat(buf, RSTRING_PTR(readed), RSTRING_LEN(readed));
	rb_fairy_debug(self, buf);
	rb_jump_tag(state);
      }
      else {
	rb_fairy_debug(self, DEBUG_MSG(5)); 
	rb_jump_tag(state);
      }
      rb_fairy_debug(self, DEBUG_MSG(6)); 
    }
    else {
      rb_fairy_debug(self, DEBUG_MSG(7)); 
      rb_jump_tag(state);
    }
  }
  return self;
}

static VALUE
rb_xmsbcb(_read_buffer_sub)(VALUE self)
{
  xmsbcb(_t) *cb;
  VALUE cache;
  
  GetFXMSBCBPtr(self, cb);

  cache = rb_marshal_load(cb->io);
  if (CLASS_OF(cache) == rb_cFairyStringBuffer) {
    cache = rb_fairy_string_buffer_to_a(cache);
  }
  rb_iv_set(self, "@cache", cache);
  return self;
}

void
Init_p_xgroup_by()
{
  VALUE pxg;
  VALUE xpf;
  VALUE msb;
  VALUE xmsb;
  VALUE xmsbcb;

  if (!rb_const_defined(rb_mFairy, rb_intern("PGroupBy"))) {
    return;
  }
  rb_cFairyPGroupBy = rb_const_get(rb_mFairy, rb_intern("PGroupBy"));
  
  id_new = rb_intern("new");
  id_init_key_proc = rb_intern("init_key_proc");
  id_yield = rb_intern("yield");
  id_each = rb_intern("each");
  id_push = rb_intern("push");
  id_add_exports = rb_intern("add_exports");
  id_start = rb_intern("start");
  id_set_njob_id = rb_intern("njob_id=");
  id_add_key = rb_intern("add_key");

  id_sort_by_bang = rb_intern("sort_by!");
  id_open_buffer = rb_intern("open_buffer");
  id_io = rb_intern("io");
  id_close = rb_intern("close");
puts("A:1");
  rb_cFairyPXGroupBy = rb_define_class_under(rb_mFairy, "PXGroupBy", rb_cFairyPGroupBy);
  pxg = rb_cFairyPXGroupBy;
  rb_define_alloc_func(pxg, fairy_p_xgroup_by_alloc);
  rb_define_method(pxg, "initialize", rb_fairy_p_xgroup_by_initialize, 5);
  rb_define_method(pxg, "add_export", rb_fairy_p_xgroup_by_add_export, 2);
  rb_define_method(pxg, "start_export", rb_fairy_p_xgroup_by_start_export, 0);

puts("A:2");
  rb_fairy_processor_def_export(rb_cFairyPXGroupBy);

  PPostFilter = rb_const_get(rb_cFairyPGroupBy, rb_intern("PPostFilter"));
  
  msb= rb_const_get(rb_cFairyPGroupBy, rb_intern("DirectMergeSortBuffer"));
  MergeSortBuffer = msb;
  MSBMerger = rb_const_get(msb, rb_intern("Merger"));
  MSBCachedBuffer = rb_const_get(msb, rb_intern("CachedBuffer"));

  xpf = rb_define_class_under(rb_cFairyPXGroupBy,
			      "PPostFilter", PPostFilter);
  rb_cFairyPXGPostfilter = xpf;
  rb_define_alloc_func(xpf, xpf(_alloc));
  rb_define_method(xpf, "initialize", rb_xpf(_initialize), 5);
  rb_define_method(xpf, "basic_each", rb_xpf(_basic_each), 0);

  rb_fairy_processor_def_export(xpf);
  
  
puts("A:3");
  xmsb = rb_define_class_under(rb_cFairyPXGroupBy,
			       "XDirectMergeSortBuffer", MergeSortBuffer);
  rb_cFairyPXGDirctMergeSortBuffer = xmsb;
  rb_define_alloc_func(xmsb, xmsb(_alloc));
  rb_define_method(xmsb, "initialize", rb_xmsb(_initialize), 2);
  rb_define_method(xmsb, "init_2ndmemory", rb_xmsb(_init_2ndmemory), 0);
  rb_define_method(xmsb, "store_2ndmemory", rb_xmsb(_store_2ndmemory), 1);
  rb_define_method(xmsb, "each_2ndmemory", rb_xmsb(_each_2ndmemory), 0);

puts("A:4");
  xmsbcb = rb_define_class_under(xmsb, "CachedBuffer", MSBCachedBuffer);
  rb_cFairyPXGDirctMergeSortBufferCachedBuffer = xmsbcb;
  rb_define_alloc_func(xmsbcb, xmsbcb(_alloc));
  rb_define_method(xmsbcb, "initialize", rb_xmsbcb(_initialize), 2);
  rb_define_method(xmsbcb, "read_buffer", rb_xmsbcb(_read_buffer), 0);
puts("A:5");
}
