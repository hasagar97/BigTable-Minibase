package iterator;
import chainexception.*;

import java.lang.*;

public class  CorruptedFieldNo extends ChainException {
    public  CorruptedFieldNo(String s){super(null,s);}
    public  CorruptedFieldNo(Exception prev, String s){ super(prev,s);}
}
