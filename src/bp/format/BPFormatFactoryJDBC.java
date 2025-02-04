package bp.format;

import java.util.function.Consumer;

public class BPFormatFactoryJDBC implements BPFormatFactory
{
	public void register(Consumer<BPFormat> regfunc)
	{
		regfunc.accept(new BPFormatSQL());
		regfunc.accept(new BPFormatJDBCLink());
	}
}
