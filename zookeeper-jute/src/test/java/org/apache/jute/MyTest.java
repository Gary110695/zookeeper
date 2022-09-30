package org.apache.jute;

import org.junit.Test;

import java.io.*;

/**
 *
 */
public class MyTest {

    @Test
    public void test() throws IOException {
        String path = "E:\\test.txt";
        // 将Person写出到文件中
        OutputStream outputStream = new FileOutputStream(new File(path));
        // 创建输出Archive
        BinaryOutputArchive binaryOutputArchive = BinaryOutputArchive.getArchive(outputStream);

        Person person = new Person(18, "jack");
        // 本质上是调用Person类的序列化方法来操作的
        binaryOutputArchive.writeRecord(person, "person");

        // 从文件中读取Person对象
        InputStream inputStream = new FileInputStream(new File(path));
        BinaryInputArchive binaryInputArchive = BinaryInputArchive.getArchive(inputStream);

        Person person2 = new Person();
        // 本质上是调用Person类的反序列化方法来操作的
        binaryInputArchive.readRecord(person2, "person");
        System.out.println(person2);    // Person{age=18, name='jack'}
    }

}

class Person implements Record {
    private int age;
    private String name;

    public Person() {
    }

    public Person(int age, String name) {
        this.age = age;
        this.name = name;
    }

    public void serialize(OutputArchive archive, String tag) throws IOException {
        // 每个以startRecord开头，endRecord结尾
        archive.startRecord(this, tag);
        archive.writeInt(age, "age");
        archive.writeString(name, "name");
        archive.endRecord(this, tag);
    }

    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        age = archive.readInt("age");
        name = archive.readString("name");
        archive.endRecord(tag);
    }

    @Override
    public String toString() {
        return "Person{" +
                "age=" + age +
                ", name='" + name + '\'' +
                '}';
    }
}