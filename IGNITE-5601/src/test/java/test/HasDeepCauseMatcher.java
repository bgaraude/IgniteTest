package test;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.internal.matchers.ThrowableCauseMatcher;

public class HasDeepCauseMatcher extends TypeSafeMatcher<Throwable> {

	private final Matcher<? extends Throwable> causeMatcher;

	public HasDeepCauseMatcher(Matcher<? extends Throwable> causeMatcher) {
		this.causeMatcher = causeMatcher;
	}

	public void describeTo(Description description) {
		description.appendText("exception with cause ");
		description.appendDescriptionOf(causeMatcher);
	}

	@Override
	protected boolean matchesSafely(Throwable item) {
		if(item == null){
			return false;
		}
		if(causeMatcher.matches(item.getCause())){
			return true;
		}
		if(item.getSuppressed()!=null){
			for(Throwable t:item.getSuppressed()){
				if(matchesSafely(t)){
					return true;
				}
			}
		}
		if(item.getCause() == item){
			return false;
		}
		return matchesSafely(item.getCause());
	}

	@Override
	protected void describeMismatchSafely(Throwable item, Description description) {
		description.appendText("cause ");
		causeMatcher.describeMismatch(item.getCause(), description);
	}

	/**
	 * Returns a matcher that verifies that the outer exception has a cause for
	 * which the supplied matcher evaluates to true.
	 *
	 * @param matcher
	 *            to apply to the cause of the outer exception
	 * @param <T>
	 *            type of the outer exception
	 */
	@Factory
	public static <T extends Throwable> Matcher<T> hasCause(final Matcher<? extends Throwable> matcher) {
		return new ThrowableCauseMatcher<T>(matcher);
	}
}
