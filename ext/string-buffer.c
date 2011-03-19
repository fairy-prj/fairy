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
  VALUE buffer;
} fairy_string_buffer_t;

#define GetFairyStringBufferPtr(obj, tobj) \
  TypedData_Get_Struct((obj), fairy_string_buffer_t, &fairy_string_buffer_data_type, (tobj))

static void
fairy_string_buffer_mark(void *ptr)
{
  fairy_string_buffer_t *sb = (fairy_string_buffer_t*)ptr;
  
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

static const rb_data_type_t fairy_string_buffer_data_type = {
    "fairy_string_buffer",
    {fairy_string_buffer_mark, fairy_string_buffer_free, fairy_string_buffer_memsize,},
};

static VALUE
fairy_string_buffer_alloc(VALUE klass)
{
  VALUE volatile obj;
  fairy_string_buffer_t *sb;

  obj = TypedData_Make_Struct(klass, fairy_string_buffer_t, &fairy_string_buffer_data_type, sb);
  
  sb->buffer = rb_str_buf_new(STRING_BUFFER_CAPA);

  return obj;
}

static VALUE
fairy_string_buffer_initialize(VALUE self)
{
  return self;
}

VALUE
rb_fairy_string_buffer_new(void)
{
  VALUE sb;
  sb = fairy_string_buffer_alloc(rb_cFairyStringBuffer);
  fairy_string_buffer_initialize(sb);
  return sb;
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
  
  rb_str_buf_cat(sb->buffer, RSTRING_PTR(str), RSTRING_LEN(str));
  rb_str_buf_cat(sb->buffer, "\n", 1);
  sb->size++;
  return self;
}

VALUE
rb_fairy_string_buffer_to_a(VALUE self)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);

  return rb_str_split(sb->buffer, "\n");
}

VALUE
rb_fairy_string_buffer_marshal_dump(VALUE self)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);

  return rb_ary_new3(2, LONG2NUM(sb->size), sb->buffer);
}


VALUE
rb_fairy_string_buffer_marshal_load(VALUE self, VALUE obj)
{
  fairy_string_buffer_t *sb;
  GetFairyStringBufferPtr(self, sb);
  sb->size = NUM2LONG(rb_ary_entry(obj, 0));
  sb->buffer = rb_ary_entry(obj, 1);
  return self;
}

Init_string_buffer()
{
  rb_cFairyStringBuffer = rb_define_class_under(rb_mFairy, "StringBuffer", rb_cObject);
  rb_define_alloc_func(rb_cFairyStringBuffer, fairy_string_buffer_alloc);
  rb_define_method(rb_cFairyStringBuffer, "initialize", fairy_string_buffer_initialize, 0);
  rb_define_method(rb_cFairyStringBuffer, "size", rb_fairy_string_buffer_size, 0);
  rb_define_method(rb_cFairyStringBuffer, "push", rb_fairy_string_buffer_push, 1);
  rb_define_method(rb_cFairyStringBuffer, "to_a", rb_fairy_string_buffer_to_a, 0);
  rb_define_method(rb_cFairyStringBuffer, "marshal_dump", rb_fairy_string_buffer_marshal_dump, 0);

  rb_define_method(rb_cFairyStringBuffer, "marshal_load", rb_fairy_string_buffer_marshal_load, 1);
}
