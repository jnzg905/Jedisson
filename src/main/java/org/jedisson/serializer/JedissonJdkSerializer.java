package org.jedisson.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.jedisson.api.IJedissonSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class JedissonJdkSerializer<T> extends JedissonAbstractSerializer<T>{

	public JedissonJdkSerializer(){
		this(null);
	}
	
	public JedissonJdkSerializer(Class<T> clss) {
		super(clss);
		// TODO Auto-generated constructor stub
	}

	@Override
	public byte[] serialize(T t) throws SerializationException {
		if(t == null){
			return null;
		}
		
		if (!(t instanceof Serializable)) {
			throw new IllegalArgumentException(getClass().getSimpleName() + " requires a Serializable payload " +
					"but received an object of type [" + t.getClass().getName() + "]");
		}
		ByteArrayOutputStream bytesStream = new ByteArrayOutputStream(1024);
		
		
		try(ObjectOutputStream objectOutputStream = new ObjectOutputStream(bytesStream)) { 
			objectOutputStream.writeObject(t);
			objectOutputStream.flush();	
		} catch (IOException e) {
			throw new SerializationException(e.getMessage(),e);
		}
		return bytesStream.toByteArray();
	}

	@Override
	public T deserialize(byte[] str) throws SerializationException {
		return deserialize(str,null);
	}

	@Override
	public T deserialize(byte[] bytes, Class<T> clss) throws SerializationException {
		if(bytes == null){
			return null;
		}
		ByteArrayInputStream bytesStream = new ByteArrayInputStream(bytes);
		try(ObjectInputStream objectInputStream = new ObjectInputStream(bytesStream)){
			return (T) objectInputStream.readObject();
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
