package net.deelam.common;

public interface Pojo<C extends Pojo<C>> {

   C copy();

}
