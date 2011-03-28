/**********************************************************************

  p-xgroup-by.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"
#include "xthread.h"

#include "fairy.h"

static ID id_new;
static ID id_each;
static ID id_push;
static ID id_add_exports;
static ID id_start;
static ID id_set_njob_id;
static ID id_add_key;

static VALUE rb_cFairyPGroupBy = Qnil;

VALUE rb_cFairyPXGroupBy;


typedef struct rb_fairy_p_xgroup_by_struct
{
  VALUE bjob;
  VALUE input;
  VALUE opts;
  VALUE id;
  
  long mod;
  
  VALUE postqueuing_policy;
  VALUE exports_queue;
  
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

  rb_gc_mark(gb->exports_queue);
  
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
  
  gb->postqueuing_policy = Qnil;
  gb->exports_queue = Qnil;
  
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
  rb_fairy_debug(self, "START_EXPORT");

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
  unsigned int key;
  VALUE export;

  GetFairyPXGroupByPtr(self, gb);

  if (CLASS_OF(e) == rb_cFairyImportCTLTOKEN_NULLVALUE) {
    return self;
  }

  key = rb_fairy_simple_hash_uint(rb_mFairySimpleHash, e) % gb->mod;
  export = gb->exports[key];
  if (NIL_P(export)) {
    export = rb_class_new_instance(1, &gb->postqueuing_policy, rb_cFairyExport);
    rb_funcall(export, id_set_njob_id, 1, gb->id);
    rb_funcall(export, id_add_key, 1, INT2FIX(key));
    rb_fairy_p_xgroup_by_add_export(self, key, export);
  }
  rb_funcall(export, id_push, 1, e);
  gb->counter[key]++;
  return self;
}

void
Init_p_xgroup_by()
{
  VALUE pxg;

  if (!rb_const_defined(rb_mFairy, rb_intern("PGroupBy"))) {
    return;
  }
  rb_cFairyPGroupBy = rb_const_get(rb_mFairy, rb_intern("PGroupBy"));
  
  id_new = rb_intern("new");
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
}






