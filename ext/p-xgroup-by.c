/**********************************************************************

  p-xgroup-by.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"
#include "xthread.h"

#include "fairy.h"

static ID id_init_key_proc;
static ID id_yield;
static ID id_each;
static ID id_push;
static ID id_add_exports;
static ID id_start;
static ID id_set_njob_id;
static ID id_add_key;

static ID id_close;

static VALUE rb_cFairyPGroupBy;
static VALUE PPostFilter;

VALUE rb_cFairyPXGroupBy;
VALUE rb_cFairyPXGPostfilter;

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
  VALUE opts;
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
  rb_gc_mark(pf->opts);
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

  pf->opts = Qnil;
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
  VALUE buf_klass_name;

  VALUE argv[] = {
    id, ntask,  bjob, opts, block_source,
  };
  GetFXPFPtr(self, pf);
  rb_call_super(5, argv);

  pf->opts = rb_iv_get(self, "@opts");
  pf->buffering_policy = rb_fairy_conf("XGROUP_BY_BUFFERING_POLICY", pf->opts, "buffering_policy");
  buf_klass_name = rb_hash_aref(pf->buffering_policy,
				ID2SYM(rb_intern("buffering_class")));
  if (NIL_P(buf_klass_name)) {
    buf_klass_name = rb_hash_aref(rb_fairy_conf("XGROUP_BY_BUFFERING_POLICY", Qnil, NULL), ID2SYM(rb_intern("buffering_class")));
  }
   
  rb_fairy_debug_p2(self, "Buffering Class", buf_klass_name);
  pf->buffering_class = rb_const_get(rb_cFairyPXGroupBy, SYM2ID(buf_klass_name));
 
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

void
Init_p_xgroup_by()
{
  VALUE pxg;
  VALUE xpf;

  if (!rb_const_defined(rb_mFairy, rb_intern("PGroupBy"))) {
    return;
  }
  rb_cFairyPGroupBy = rb_const_get(rb_mFairy, rb_intern("PGroupBy"));
  
  id_init_key_proc = rb_intern("init_key_proc");
  id_yield = rb_intern("yield");
  id_each = rb_intern("each");
  id_push = rb_intern("push");
  id_add_exports = rb_intern("add_exports");
  id_start = rb_intern("start");
  id_set_njob_id = rb_intern("njob_id=");
  id_add_key = rb_intern("add_key");

  rb_cFairyPXGroupBy = rb_define_class_under(rb_mFairy, "PXGroupBy", rb_cFairyPGroupBy);
  pxg = rb_cFairyPXGroupBy;
  rb_define_alloc_func(pxg, fairy_p_xgroup_by_alloc);
  rb_define_method(pxg, "initialize", rb_fairy_p_xgroup_by_initialize, 5);
  rb_define_method(pxg, "add_export", rb_fairy_p_xgroup_by_add_export, 2);
  rb_define_method(pxg, "start_export", rb_fairy_p_xgroup_by_start_export, 0);

  rb_fairy_processor_def_export(rb_cFairyPXGroupBy);

  PPostFilter = rb_const_get(rb_cFairyPGroupBy, rb_intern("PPostFilter"));
  
  xpf = rb_define_class_under(rb_cFairyPXGroupBy,
			      "PPostFilter", PPostFilter);
  rb_cFairyPXGPostfilter = xpf;
  rb_define_alloc_func(xpf, xpf(_alloc));
  rb_define_method(xpf, "initialize", rb_xpf(_initialize), 5);
  rb_define_method(xpf, "basic_each", rb_xpf(_basic_each), 0);

  rb_fairy_processor_def_export(xpf);
  
  
}
