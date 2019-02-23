package com.pingcap.common;

public class Wrapper<T> {
  public T a = null;

  public Wrapper(T a) {
    this.a = a;
  }

  public Wrapper() {}
}
