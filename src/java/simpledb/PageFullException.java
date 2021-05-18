package simpledb;

public class PageFullException extends Exception{
    public PageFullException(String message) {
        super(message);
    }
}
