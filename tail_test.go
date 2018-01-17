package tail

import (
	"context"
	"reflect"
	"testing"
)

func TestTailPush(t *testing.T) {

	t.Run("fixed", func(t *testing.T) {
		v := New(3)

		for i := 0; i < 10; i++ {
			v.Push(i)
		}

		if !reflect.DeepEqual(v.(*tail).fixedData[0], []interface{}{9, nil, nil}) {
			t.FailNow()
		}

		if !reflect.DeepEqual(v.(*tail).fixedData[1], []interface{}{6, 7, 8}) {
			t.FailNow()
		}

		if v.(*tail).next != 10 {
			t.FailNow()
		}
	})
}

func MakeGetAssert(t *testing.T, obj Tail) func(context.Context, uint64, uint64, []interface{}, uint64) {
	return func(ctx context.Context, offset uint64, limit uint64, expectedResult []interface{}, expectedOffset uint64) {
		actualResult, actualOffset := obj.Get(ctx, offset, limit)

		if actualOffset != expectedOffset {
			t.Fatalf("%d (actual) != %d (expected)", actualOffset, expectedOffset)
		}
		if !reflect.DeepEqual(actualResult, expectedResult) {
			t.Fatalf("%#v (actual) != %#v (expected)", actualResult, expectedResult)
		}
	}
}

func TestTailGet(t *testing.T) {
	t.Run("fixed", func(t *testing.T) {
		v := New(3)

		for i := 0; i < 8; i++ {
			v.Push(i)
		}

		bg := context.Background()

		t.Run("t1", func(t *testing.T) {
			get := MakeGetAssert(t, v)
			get(bg, 0, 100, []interface{}{3, 4, 5}, 6)
		})
		t.Run("t2", func(t *testing.T) {
			get := MakeGetAssert(t, v)
			get(bg, 3, 100, []interface{}{3, 4, 5}, 6)
		})
		t.Run("t3", func(t *testing.T) {
			get := MakeGetAssert(t, v)
			get(bg, 5, 100, []interface{}{5}, 6)
		})
		t.Run("t4", func(t *testing.T) {
			get := MakeGetAssert(t, v)
			get(bg, 6, 100, []interface{}{6, 7}, 8)
		})
		t.Run("t5", func(t *testing.T) {
			get := MakeGetAssert(t, v)
			get(bg, 2, 2, []interface{}{3, 4}, 5)
		})
	})

}

func BenchmarkPush(b *testing.B) {
	fixedSize := uint64(10000)
	value := struct{}{}

	b.Run("fixed", func(b *testing.B) {
		v := New(fixedSize)

		for i := 0; i <= b.N; i++ {
			v.Push(value)
		}
	})
}

func BenchmarkGet(b *testing.B) {
	size := uint64(10000)
	value := struct{}{}
	bg := context.Background()

	fixed := New(size)

	for i := uint64(0); i < 2*size; i++ {
		fixed.Push(value)
	}

	b.Run("fixed", func(b *testing.B) {
		for i := 0; i <= b.N; i++ {
			_, n := fixed.Get(bg, 5, 100)
			if n != 105 {
				b.FailNow()
			}
		}
	})
}
