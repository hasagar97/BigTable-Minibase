package iterator;
import chainexception.*;

import java.lang.*;

public class InvalidFieldSize extends ChainException {
    public InvalidFieldSize(String s){super(null,s);}
    public InvalidFieldSize(Exception prev, String s){ super(prev,s);}
}
