import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class CodecTest {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Lucene codec initialization...");
            
            // This should work without throwing ServiceConfigurationError
            Codec defaultCodec = Codec.getDefault();
            System.out.println("Default codec loaded successfully: " + defaultCodec.getName());
            
            // Test IndexWriterConfig creation (this was failing before)
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            System.out.println("IndexWriterConfig created successfully");
            
            System.out.println("✅ SUCCESS: Codec fix resolved the issue!");
            
        } catch (Exception e) {
            System.err.println("❌ FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
