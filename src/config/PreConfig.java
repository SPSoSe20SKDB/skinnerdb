package config;

/**
 * Configuration parameters influencing the
 * pre-processing phase.
 * 
 * @author immanueltrummer
 *
 */
public class PreConfig {
	/**
	 * Whether to apply unary predicates for filtering
	 * during pre-processing step.
	 */
	public static final boolean PRE_FILTER = true;
	/**
	 * Whether to consider using indices for evaluating
	 * unary equality predicates.
	 */
	public static boolean CONSIDER_INDICES = false;
}
