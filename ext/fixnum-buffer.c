/**********************************************************************

  fixnum-buffer.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"

#include "fairy.h"

#define FIXNUM_BUFFER_DEFAULT_CAPA 16

VALUE rb_cFairyFixnumBuffer;

typedef struct rb_fairy_fixnum_buffer_struct
{
  long capa;
  long length;
  long push;
  long pop;
  unsigned char *fixnums;
} fairy_fixnum_buffer_t;

#define GetFairyFixnumBufferPtr(obj, tobj) \
  TypedData_Get_Struct((obj), fairy_fixnum_buffer_t, &fairy_fixnum_buffer_data_type, (tobj))

static void
fairy_fixnum_buffer_free(void *ptr)
{
  fairy_fixnum_buffer_t *fb = (fairy_fixnum_buffer_t*)ptr;
  
  ruby_xfree(fb->fixnums);
  ruby_xfree(ptr);
}

static size_t
fairy_fixnum_buffer_memsize(const void *ptr)
{
  fairy_fixnum_buffer_t *fb = (fairy_fixnum_buffer_t*)ptr;
  return ptr ? sizeof(fairy_fixnum_buffer_t) + fb->capa : 0;
}

static const rb_data_type_t fairy_fixnum_buffer_data_type = {
  "fairy_fixnum_buffer",
  {0, fairy_fixnum_buffer_free, fairy_fixnum_buffer_memsize,},
};

static VALUE
fairy_fixnum_buffer_alloc(VALUE klass)
{
  VALUE volatile obj;
  fairy_fixnum_buffer_t *fb;

  obj = TypedData_Make_Struct(klass, fairy_fixnum_buffer_t, &fairy_fixnum_buffer_data_type, fb);
  
  fb->capa = FIXNUM_BUFFER_DEFAULT_CAPA;
  fb->length = 0;
  fb->push = 0;
  fb->pop = 0;
  fb->fixnums = NULL;

  return obj;
}

static void
fairy_fixnum_buffer_resize_double_capa(fairy_fixnum_buffer_t *fb)
{
  long new_capa = fb->capa * 2;

puts("dc: 1");  
  REALLOC_N(fb->fixnums, unsigned char, new_capa);
puts("dc: 2");  
  
  if (fb->push > fb->capa) {
puts("dc: 3");  
    if (fb->capa - fb->pop <= fb->push - fb->capa) {
puts("dc: 4");  
      MEMCPY(&fb->fixnums[fb->pop + fb->capa],
	     &fb->fixnums[fb->pop], unsigned char, fb->capa - fb->pop);
puts("dc: 5");  
      fb->pop += fb->capa;
      fb->push += fb->capa;
    }
    else {
puts("dc: 6");  
      MEMCPY(&fb->fixnums[fb->capa],
	     fb->fixnums, unsigned char, fb->push - fb->capa);
puts("dc: 7");  
    }
puts("dc: 8");  
  }
puts("dc: 9");  
  fb->capa = new_capa;
}

static VALUE
fairy_fixnum_buffer_initialize(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  GetFairyFixnumBufferPtr(self, fb);
  
  fb->fixnums = ALLOC_N(unsigned char, fb->capa);
  return self;
}

VALUE
rb_fairy_fixnum_buffer_new(void)
{
  return fairy_fixnum_buffer_alloc(rb_cFairyFixnumBuffer);
}

VALUE
rb_fairy_fixnum_buffer_length(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  
  GetFairyFixnumBufferPtr(self, fb);
  return LONG2NUM(fb->length);
}


VALUE
rb_fairy_fixnum_buffer_realsize(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  
  GetFairyFixnumBufferPtr(self, fb);
  return LONG2NUM(fb->push - fb->pop);
}

inline static int
push_index(fairy_fixnum_buffer_t *fb)
{
  int idx = 0;
  retry:
  if (fb->push < fb->capa) {
    idx = fb->push;
  }
  else if (fb->push - fb->capa < fb->pop) {
    idx = fb->push - fb->capa;
  }
  else {
    fairy_fixnum_buffer_resize_double_capa(fb);
    goto retry;
  }
  return idx;
}

VALUE
rb_fairy_fixnum_buffer_push_long(VALUE self, long v)
{
  fairy_fixnum_buffer_t *fb;
  int idx, i;
  char low;
  GetFairyFixnumBufferPtr(self, fb);
  
  fb->length++;
  for (i = 0; i <= (int)sizeof(long); i++) {
    idx = push_index(fb);
    low = (char)(v & 0x7f); 
    v >>= 7;
    if (v == 0 && low != 0x7f) {
      fb->fixnums[idx] = low;
      fb->push++;
      return self;
    }
    else if (v == -1) {
      low |= 0x80;
      fb->fixnums[idx] = low;
      fb->push++;
      idx = push_index(fb);
      fb->fixnums[idx] = 0x7f;
      fb->push++;
      return self;
    }
    else { 
      low |= 0x80; 
      fb->fixnums[idx] = low;
      fb->push++;
    } 
  }
  return self;
}


VALUE
rb_fairy_fixnum_buffer_push(VALUE self, VALUE v)
{
  if (!FIXNUM_P(v)) {
    rb_raise(rb_eTypeError, "not a fixnum");
  }

  return rb_fairy_fixnum_buffer_push_long(self, FIX2LONG(v));
}

inline static long
at_long(fairy_fixnum_buffer_t *fb, long idx, long *ret)
{
  long v = 0;
  long idx2;
  int i = 0;
  char low;
  
  while(1) {
    if (idx < fb->capa) {
      idx2 = idx;
    }
    else {
      idx2 = idx - fb-> capa;
    }
    low = fb->fixnums[idx2];
    idx++;

    if (low & 0x80) {
      v |= (low & 0x7f) << i;
    }
    else if (low == 0x7f) {
      while (i <= (int)sizeof(long)*8) {
	v |= 0x7f << i;
	i += 7;
      }
      break;
    }
    else {
      v |= (low & 0x7f) << i;
      break;
    }
    i += 7;
  }
  if (ret != NULL) {
    *ret = idx;
  }
  return v;
}

long
rb_fairy_fixnum_buffer_pop_long(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  long v = 0;
  int i;
  char low;
  
  GetFairyFixnumBufferPtr(self, fb);

  if (fb->push == fb->pop)
    return 0;

  fb->length--;
  i = 0;
  while(1) {
    low = fb->fixnums[fb->pop++];
    if(fb->pop >= fb->capa) {
      fb->pop -= fb->capa;
      fb->push -= fb->capa;
    }

    if (low & 0x80) {
      v |= (low & 0x7f) << i;
    }
    else if (low == 0x7f) {
      while (i <= (int)sizeof(long)*8) {
	v |= 0x7f << i;
	i += 7;
      }
      break;
    }
    else {
      v |= (low & 0x7f) << i;
      break;
    }
    i += 7;
  }
  return v;
}

VALUE
rb_fairy_fixnum_buffer_pop(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  GetFairyFixnumBufferPtr(self, fb);
  
  if (fb->push == fb->pop)
    return Qnil;
  return LONG2FIX(rb_fairy_fixnum_buffer_pop_long(self));
}

VALUE
rb_fairy_fixnum_buffer_each(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  long i;
  
  GetFairyFixnumBufferPtr(self, fb);
  
  i = fb->pop;
  while (i < fb->push) {
    rb_yield(LONG2FIX(at_long(fb, i, &i)));
  }
  return self;
} 

VALUE
rb_fairy_fixnum_buffer_to_a(VALUE self)
{
  VALUE ary;
  fairy_fixnum_buffer_t *fb;
  VALUE s, v;
  long i;
  
  GetFairyFixnumBufferPtr(self, fb);

  ary = rb_ary_new2(fb->length);

  i = fb->pop;
  while (i < fb->push) {
    v = LONG2FIX(at_long(fb, i, &i));
    rb_ary_push(ary, v);
  }
  return ary;
}

VALUE
rb_fairy_fixnum_buffer_marshal_dump(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  long len;
  VALUE str;

  GetFairyFixnumBufferPtr(self, fb);

  if (fb->push < fb->capa) {
    str = rb_str_new(fb->fixnums + fb->pop, fb->push - fb->pop);
  }
  else {
    str = rb_str_new(fb->fixnums + fb->pop, fb->capa - fb->pop);
    rb_str_cat(str, fb->fixnums, fb->push - fb->capa);
  }
  return rb_ary_new3(3, LONG2NUM(fb->push - fb->pop), str, LONG2NUM(fb->length));
}

VALUE
rb_fairy_fixnum_buffer_marshal_load(VALUE self, VALUE obj)
{
  fairy_fixnum_buffer_t *fb;
  VALUE str;
  GetFairyFixnumBufferPtr(self, fb);

  fb->push = NUM2LONG(rb_ary_entry(obj, 0));

  fb->capa = FIXNUM_BUFFER_DEFAULT_CAPA;
  if (fb->capa <= fb->push) {
    fb->capa += fb->push;
  }

  fb->fixnums = ALLOC_N(unsigned char, fb->capa);
  MEMCPY(fb->fixnums, RSTRING_PTR(rb_ary_entry(obj, 1)),
	 unsigned char, fb->push);

  fb->length = NUM2LONG(rb_ary_entry(obj, 2));
  
  return self;
}


VALUE
rb_fairy_fixnum_buffer_inspect(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  VALUE str;
  GetFairyFixnumBufferPtr(self, fb);

  str = rb_sprintf("<%s:%x length=%d ", rb_obj_classname(self), (void*)self, fb->length);

  rb_str_append(str, rb_inspect(rb_fairy_fixnum_buffer_to_a(self)));
  rb_str_cat2(str, ">");
  return str;
}
    
VALUE
rb_fairy_fixnum_buffer_inspect_raw(VALUE self)
{
  fairy_fixnum_buffer_t *fb;
  VALUE str;
  GetFairyFixnumBufferPtr(self, fb);

  str = rb_sprintf("<%s:%x length=%d: [", rb_obj_classname(self), (void*)self, fb->length);
  if (fb->push < fb->capa) {
    VALUE s;
    long i;
    
    for (i = fb->pop; i < fb->push; i++) {
      int b = fb->fixnums[i] >> 7;
      char v = fb->fixnums[i] & 0x7f;
      
      s = rb_sprintf(" %d:%d", b, v);
      rb_str_append(str, s);
    }
  }
  else {
    VALUE s;
    long i;
    for (i = fb->pop; i < fb->capa; i++) {
      int b = fb->fixnums[i] >> 7;
      char v = fb->fixnums[i] & 0x7f;
      
      s = rb_sprintf(" %d:%d", b, v);
      rb_str_append(str, s);
    }

    for (i = 0; i < fb->push - fb->capa; i++) {
      int b = fb->fixnums[i] >> 7;
      char v = fb->fixnums[i] & 0x7f;
      
      s = rb_sprintf(" %d:%d", b, v);
      rb_str_append(str, s);
    }
  }
  rb_str_cat2(str, "]>");
  return str;
}

void
Init_fixnum_buffer()
{
  rb_cFairyFixnumBuffer = rb_define_class_under(rb_mFairy, "FixnumBuffer", rb_cObject);
  
  rb_define_alloc_func(rb_cFairyFixnumBuffer, fairy_fixnum_buffer_alloc);
  rb_define_method(rb_cFairyFixnumBuffer, "initialize", fairy_fixnum_buffer_initialize, 0);

  rb_define_method(rb_cFairyFixnumBuffer, "length", rb_fairy_fixnum_buffer_length, 0);
  rb_define_alias(rb_cFairyFixnumBuffer,  "size", "length");
  rb_define_method(rb_cFairyFixnumBuffer, "realsize", rb_fairy_fixnum_buffer_realsize, 0);
  rb_define_method(rb_cFairyFixnumBuffer, "push", rb_fairy_fixnum_buffer_push, 1);
  rb_define_method(rb_cFairyFixnumBuffer, "pop", rb_fairy_fixnum_buffer_pop, 0);
  rb_define_method(rb_cFairyFixnumBuffer, "each", rb_fairy_fixnum_buffer_each, 0);
  rb_define_method(rb_cFairyFixnumBuffer, "to_a", rb_fairy_fixnum_buffer_to_a, 0);
  rb_define_method(rb_cFairyFixnumBuffer, "marshal_dump", rb_fairy_fixnum_buffer_marshal_dump, 0);
  rb_define_method(rb_cFairyFixnumBuffer, "marshal_load", rb_fairy_fixnum_buffer_marshal_load, 1);
  rb_define_method(rb_cFairyFixnumBuffer, "inspect", rb_fairy_fixnum_buffer_inspect, 0);
}
