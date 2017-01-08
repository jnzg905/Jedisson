package org.jedisson.api;

import java.io.Serializable;

public interface IJedissonClosure<E> extends Serializable {

	public void apply(E e);
}
