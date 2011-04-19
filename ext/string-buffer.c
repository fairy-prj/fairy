/**********************************************************************

  string-buffer.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"

#include "fairy.h"

#define STRING_BUFFER_CAPA 10240

VALUE rb_cFairyStringBuffer;

typedef struct rb_fairy_string_buffer_struct
{
  long size;
  VALUE string_sizes;
  VALUE buffer;
} fairy_string_buffer_t;

#define GetFairyStringBufferPtr(obj, tobj) \
  TypedData_Get_Struct((obj), fairy_string_buffer_t, &fairy_string_buffer_data_type, (tobj))

static void
fairy_string_buffer_mark(void *ptr)
{
  fairy_string_buffer_t *sb = (fairy_string_buffer_t*)ptr;
  
  rb_gc_mark(sb->string_sizes);
  rb_gc_mark(sb->buffer);
}

static void
fairy_string_buffer_free(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
fairy_string_buffer_memsize(const void *ptr)
{
  return ptr ? sizeof(fairy_string_buffer_t) : 0;
}

#ifdef HAVE_RB_DATA_TYPE_T_FUNCTION
static const rb_data_type_t fairy_string_buffer_data_type = {
    "fairy_string_buffer",
    {fairy_string_buffer_mark, fairy_string_buffer_free, fairy_string_buffer_memsize,},
};
#else
static const rb_data_type_t fairy_string_buffer_data_type = {
    "fairy_string_buffer",
    fairy_string_buffer_mark,
    fairy_string_buffer_free,
    fairy_string_buffer_memsize,
};
#endif

static VALUE
fairy_string_buffer_alloc(VALUE klass)
{
  VALUE volatile obj;
  fairy_string_buffer_t *sb;

  obj = TypedData_Make_Struct(klass, fairy_string_buffer_t, &fairy_string_buffer_data_type, sb);

  sb->size = 0;
  sb->string_sizes = Qnil;
  sb->buffer = Qnil;

  return obj;
}

static VALUE
rb_fairy_string_buffer_initialize(VALUE self)
{
  fairy_string_buffer_t *sb;
  
  GetFairyStringBufferPtr(self, sb);
  sb->string_sizes = rb_fairy_fixnum_buffer_new();
  sb->buffer = rb_str_buf_new(STRING_BUFFER_CAPA);
  return self;
}

static VALUE
fairy_string_buffer_initialize(int argc, VALUE *argv, VALUE self)
{
  rb_fairy_string_buffer_initialize(self);
  if (argc == 0) {
    /* do nothing */
  }
  else if (argc == 1 && CLASS_OF(argv[0]) == rb_cArray) {
    VALUE ary = argv[0];
    long i;
      
    for (i = 0; i < RARRAY_LEN(ary); i++) {
      rb_fairy_string_buffer_push(self, RARRAY_PTR(ary)[i]);
    }
  }
  else {
    int i;
    for (i = 0; i < argc; i++) {
      rb_fairy_string_buffer_push(self, argv[i]);
    }
  }
  return self;
}


VALUE
rb_fairy_string_buffer_new(void)
{
  VALUE sb;
  sb = fairy_string_buffer_alloc(rb_cFairyStringBuffer);
  rb_fairy_string_buffer_initialize(sb);
  return sb;
}

VALUE
rb_fairy_string_buffer_new2(VALUE ary)
{
  VALUE sb;
  long i;
  sb = rb_fairy_string_buffer_new();

  for (i = 0; i < RARRAY_LEN(ary); i++) {
    rb_fairy_string_buffer_push(sb, RARRAY_PTR(ary)[i]);
  }
  
  return sb;
}

VALUE
rb_fairy_string_buffer_clear(VALUE self)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);

  sb->size = 0;
  rb_fairy_fixnum_buffer_clear(sb->string_sizes);
  rb_str_replace(sb->buffer, rb_str_new2(""));

  return self;
  
}

VALUE
rb_fairy_string_buffer_size(VALUE self)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);

  return LONG2NUM(sb->size);
}

VALUE
rb_fairy_string_buffer_push(VALUE self, VALUE str)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);

  if (!RB_TYPE_P(str, T_STRING)) 
    rb_raise(rb_eTypeError, "wrong argument type (expected String)");
  rb_fairy_fixnum_buffer_push_long(sb->string_sizes, RSTRING_LEN(str));
  rb_str_buf_cat(sb->buffer, RSTRING_PTR(str), RSTRING_LEN(str));
  sb->size++;
  return self;
}

struct each_arg {
  char *current;
};

static VALUE
each_proc(long str_size, VALUE v)
{
  struct each_arg *arg = (struct each_arg *)v;
  VALUE str;
  VALUE ret;
  
  str = rb_str_new(arg->current, str_size);
  ret = rb_yield(str);
  arg->current += str_size;
  return ret;
}


VALUE
rb_fairy_string_buffer_each(VALUE self)
{
  fairy_string_buffer_t *sb;
  struct each_arg arg;
    
  GetFairyStringBufferPtr(self, sb);

  arg.current = RSTRING_PTR(sb->buffer);
  rb_fairy_fixnum_buffer_each_callback(sb->string_sizes,
				       each_proc, (VALUE)&arg);
  return self;
}

struct to_a_arg {
  char *current;
  VALUE ary;
};


static VALUE
to_a_proc(long str_size, VALUE v)
{
  struct to_a_arg *arg = (struct to_a_arg *)v;
  VALUE ret;
  
  rb_ary_push(arg->ary, rb_str_new(arg->current, str_size));
  arg->current += str_size;
  return arg->ary;
}

VALUE
rb_fairy_string_buffer_to_a(VALUE self)
{
  fairy_string_buffer_t *sb;
  struct to_a_arg arg;
    
  GetFairyStringBufferPtr(self, sb);

  arg.current = RSTRING_PTR(sb->buffer);
  arg.ary = rb_ary_new2(sb->size);
  
  rb_fairy_fixnum_buffer_each_callback(sb->string_sizes,
				       to_a_proc, (VALUE)&arg);
  return arg.ary;
}

VALUE
rb_fairy_string_buffer_marshal_dump(VALUE self)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);

  return rb_ary_new3(3, LONG2NUM(sb->size), sb->string_sizes, sb->buffer);
}


VALUE
rb_fairy_string_buffer_marshal_load(VALUE self, VALUE obj)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);
  sb->size = NUM2LONG(rb_ary_entry(obj, 0));
  sb->string_sizes = rb_ary_entry(obj, 1);
  sb->buffer = rb_ary_entry(obj, 2);
  return self;
}

VALUE
rb_fairy_string_buffer_inspect(VALUE self)
{
  fairy_string_buffer_t *sb;
  VALUE str;
  
  GetFairyStringBufferPtr(self, sb);
  
  str = rb_sprintf("<%s:%x size=%d>", rb_obj_classname(self), (void*)self, sb->size);
  return str;
}


Init_string_buffer()
{
  VALUE fsb;
  rb_cFairyStringBuffer = rb_define_class_under(rb_mFairy, "StringBuffer", rb_cObject);

  fsb = rb_cFairyStringBuffer;
  rb_define_alloc_func(fsb, fairy_string_buffer_alloc);
  rb_define_method(fsb, "initialize", fairy_string_buffer_initialize, -1);
  rb_define_method(fsb, "size", rb_fairy_string_buffer_size, 0);
  rb_define_method(fsb, "push", rb_fairy_string_buffer_push, 1);
  rb_define_method(fsb, "each", rb_fairy_string_buffer_each, 0);
  rb_define_method(fsb, "to_a", rb_fairy_string_buffer_to_a, 0);
  rb_define_method(fsb, "marshal_dump", rb_fairy_string_buffer_marshal_dump, 0);
  rb_define_method(fsb, "marshal_load", rb_fairy_string_buffer_marshal_load, 1);
  rb_define_method(fsb, "inspect", rb_fairy_string_buffer_inspect, 0);
}
