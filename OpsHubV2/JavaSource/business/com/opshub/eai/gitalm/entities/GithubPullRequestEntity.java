package com.opshub.eai.gitalm.entities;

import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.opshub.eai.EAIComment;
import com.opshub.eai.EAIEntityRefrences;
import com.opshub.eai.EAIKeyValue;
import com.opshub.eai.EAILinkEntityItem;
import com.opshub.eai.OIMCriteriaStorageInformation;
import com.opshub.eai.core.carriers.EntityPrimaryDetailsCarrier;
import com.opshub.eai.core.carriers.MaxEntityCarrier;
import com.opshub.eai.core.carriers.NonHistoryBasedInfoCarrier;
import com.opshub.eai.core.carriers.PollingEventType;
import com.opshub.eai.core.carriers.ProcessingCarrier;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.core.utility.EAICommentComparator;
import com.opshub.eai.gitalm.adapter.GithubConnector;
import com.opshub.eai.gitalm.common.GithubConstants;
import com.opshub.eai.gitalm.common.GithubConstants.CommentTypeRequestParam;
import com.opshub.eai.gitalm.common.GithubRestRequester;
import com.opshub.eai.gitalm.common.GithubUrlBuilderHandler;
import com.opshub.eai.gitalm.data.Commit;
import com.opshub.eai.gitalm.data.GitHubComment;
import com.opshub.eai.gitalm.data.GithubPullRequest;
import com.opshub.eai.gitalm.data.GithubPullRequestSummary;
import com.opshub.eai.gitalm.data.GithubPullRequests;
import com.opshub.eai.gitalm.exceptions.GithubConnectorException;
import com.opshub.eai.gitalm.exceptions.GithubEntityObjectNotFoundException;
import com.opshub.eai.gitalm.exceptions.OIMGithubPollerException;
import com.opshub.eai.gitalm.metadata.impl.GithubPullRequestFieldsMeta;
import com.opshub.eai.metadata.AttachmentMeta;
import com.opshub.eai.metadata.CommentsMeta;
import com.opshub.eai.metadata.ExtendedCommentsMeta;
import com.opshub.eai.metadata.FieldsMeta;
import com.opshub.eai.metadata.UserMeta;
import com.opshub.eai.reqhandler.builder.UrlBuilder;
import com.opshub.eai.test.AutomationDataHandler;
import com.opshub.exceptions.eai.EAIPollerException;
import com.opshub.exceptions.eai.OIMRunTimeException;
import com.opshub.logging.OpsHubLoggingUtil;

/**
 * Github pull request entity handler. It implements all the methods that are
 * specific to github pull request.
 * 
 * Algorithm to get entities changed after -
 * 
 * 1. Given aftertime and maxtime and window size
 * 		a. Create a window from aftertime and windowmaxtime to get pull requests in that window.
 * 			1. Update aftertime to windowmaxtime.
 * 			2. If pull request for this window is empty, then
 * 				a) Get pull requests updated between aftertime and maxtime.
 * 				b) If not found, then return empty list.
 * 				c) If found, then update aftertime and windowmaxtime to found entity updated time
 * 				d) Call getNextPullRequest method again.
 * 			3. If pull request is not empty, then
 * 				1) If incomplete results not found, then 
 * 					a) Check whether total count is greater than 100. If yes, then pagination is required.
 * 					b) If yes, then fetch all pages of the pull requests in between aftertime and windowmaxtime
 * 					c) If no, then return the list of pull requests fetched between aftertime and windowmaxtime.
 * 				2) If incomplete results found, then 
 * 					a) Update windowmaxtime to half and fetch again. 
 * 					b) This continues till we either get a set of complete results. If not, then it waits till one day is left in between aftertime and windowmaxtime. 
 * 			 
 * 
 */
public class GithubPullRequestEntity implements GithubEntityHandler {

	private static final String CREATED_USER = "user";

	private final GithubRestRequester githubRestRequester;
	private final GithubConnectorService connectorService;
	private static final Logger LOGGER = Logger.getLogger(GithubPullRequestEntity.class);

	public GithubPullRequestEntity(final GithubRestRequester githubRestRequester,
			final GithubConnectorService connectorService) {
		this.githubRestRequester = githubRestRequester;
		this.connectorService = connectorService;
	}

	@Override
	public List<FieldsMeta> getFieldsMetadata() {
		return GithubPullRequestFieldsMeta.getFieldsMetadata();
	}

	@Override
	public MaxEntityCarrier getMaxUpdateTime(final Calendar afterTime) throws GithubConnectorException {

		UrlBuilder urlBuilder = GithubUrlBuilderHandler
				.getURLBuilderForMaxUpdateTime(githubRestRequester.getRepoNameFromId());
		List<GithubPullRequest> pullRequests = githubRestRequester.sendReadRequestWithoutPagination(urlBuilder,
				new TypeReference<List<GithubPullRequest>>() {});

		Calendar newAfterTime = Calendar.getInstance();
		newAfterTime.setTimeInMillis(afterTime.getTimeInMillis());
		String pullRequestId = "-1";
		if (CollectionUtils.isNotEmpty(pullRequests)) {
			Calendar pullRequestUpdatedDate = pullRequests.get(0).getUpdatedAt();
			if (pullRequestUpdatedDate.after(afterTime)) {
				newAfterTime.setTimeInMillis(pullRequestUpdatedDate.getTimeInMillis());
				pullRequestId = String.valueOf(pullRequests.get(0).getNumber());
			}
		}
		OpsHubLoggingUtil.debug(LOGGER, "Last updated Pull Request : " + pullRequestId, null);
		OpsHubLoggingUtil.debug(LOGGER, "After time : " + afterTime.getTime().toString() + ", Max update time : " + newAfterTime.getTime().toString(), null);
		return new MaxEntityCarrier(pullRequestId, newAfterTime, "-1");
	}

	@Override
	public CommentsMeta getCommentsMeta() {
		CommentsMeta commentMeta = new CommentsMeta();
		commentMeta.setSupportAsSourceSystem(true);
		commentMeta.setSupportAsTargetSystem(false);
		commentMeta.setCommentType(getAllCommentTypes());
		return commentMeta;
	}

	@Override
	public List<EAIKeyValue> getAllCommentTypes() {
		List<EAIKeyValue> commentTypes = new ArrayList<>();
		commentTypes.add(new EAIKeyValue(GithubConstants.GithubCommentTypes.ISSUE_COMMENTS,
				GithubConstants.GithubCommentTypes.ISSUE_COMMENTS));
		commentTypes.add(new EAIKeyValue(GithubConstants.GithubCommentTypes.REVIEW_COMMENTS,
				GithubConstants.GithubCommentTypes.REVIEW_COMMENTS));
		return commentTypes;
	}

	@Override
	public Object getEntityObject(final String internalId) throws GithubConnectorException {
		OpsHubLoggingUtil.debug(LOGGER, "Getting entity object for pull request with internal id : " + internalId,
				null);
		UrlBuilder urlBuilder = GithubUrlBuilderHandler
				.getURLBuilderForEntityObject(githubRestRequester.getRepoNameFromId(), internalId);
		GithubPullRequest githubPullRequest = handleEntityObjectResponse(urlBuilder);
		return githubPullRequest != null ? githubPullRequest.modifyEntityObject(connectorService, githubRestRequester)
				: githubPullRequest;
	}

	private GithubPullRequest handleEntityObjectResponse(final UrlBuilder urlBuilder) throws GithubConnectorException {
		try {
			return githubRestRequester.sendReadRequestWithoutPagination(urlBuilder, new TypeReference<GithubPullRequest>() {});
		} catch (GithubConnectorException ex) {
			if (ex instanceof GithubEntityObjectNotFoundException) {
				return null;
			}
			throw ex;
		}
	}

	@Override
	public Map<String, Object> getSystemProperties(final String internalId, final Object entityObject)
			throws GithubConnectorException {
		GithubPullRequest pullRequestEntityObject = null;
		if (entityObject != null) {
			pullRequestEntityObject = (GithubPullRequest) entityObject;
		} else {
			pullRequestEntityObject = (GithubPullRequest) getEntityObject(internalId);
		}
		return pullRequestEntityObject != null
				? pullRequestEntityObject.constructPullRequestPropertiesMap(
						connectorService.getGithubConnectorContext().getRegExpression(), String.class)
				: new HashMap<>();
	}

	@Override
	public Object getPropertyValue(final String internalId, final String propertyName)
			throws GithubConnectorException {
		Map<String, Object> sysProps = getSystemProperties(internalId, getEntityObject(internalId));
		return sysProps.get(propertyName);
	}

	@Override
	public Iterator<? extends ProcessingCarrier> getEntitiesChangedAfter(final Calendar afterTime,
			final Calendar maxTime, final String lastProcessedId,
			final OIMCriteriaStorageInformation criteriaStorageInfo, final GithubConnector githubConnector) throws OIMGithubPollerException {
		// This will create a pull request window object. This object will be
		// responsible for windowing between given after time and max time.
		GithubPullRequestWindow windowSupport = new GithubPullRequestWindow(afterTime, maxTime,
				connectorService.getGithubConnectorContext().getPollingWindowSize(), null);
		LOGGER.debug("Pull Request Window created with start time: " + afterTime.getTime().toString()
				+ " and finish time is: " + maxTime.getTime().toString());
		// Call the iterator which will handle the windowing object and the pull
		// request list present in a given window.
		return new GithubPullRequestEventIterator(windowSupport, lastProcessedId, afterTime);
	}

	@Override
	public Iterator<EntityPrimaryDetailsCarrier> getAllEntitiesMeetingCriteria(final String query,
			final Calendar updatedAfterTime, final Calendar updatedbeforeTime) throws OIMGithubPollerException {
		GithubPullRequestWindow windowSupport = new GithubPullRequestWindow(updatedAfterTime, updatedbeforeTime,
				connectorService.getGithubConnectorContext().getPollingWindowSize(), query);
		LOGGER.debug("Pull Request Window created with start time: " + updatedAfterTime.getTime().toString()
				+ " and finish time: " + updatedbeforeTime.getTime().toString());
		// Call the iterator which gets the pull
		// request list present in between after time and max time when given a
		// particular criteria.
		return new GithubPullRequestCriteriaIterator(windowSupport);
	}

	private class GithubPullRequestCriteriaIterator implements Iterator<EntityPrimaryDetailsCarrier> {

		private final GithubPullRequestWindow pullRequestWindow;
		private List<GithubPullRequestSummary> pullRequests;

		public GithubPullRequestCriteriaIterator(final GithubPullRequestWindow pullRequestWindow) {
			this.pullRequestWindow = pullRequestWindow;
			pullRequests = Collections.emptyList();
		}

		@Override
		public boolean hasNext() {
			if (CollectionUtils.isNotEmpty(pullRequests)) {
				return true;
			}
			try {
				while (pullRequests.isEmpty() && pullRequestWindow.isWindowingLeft()) {
					LOGGER.debug("Pull requests is empty. Hence, fetching from next window");
					pullRequests = pullRequestWindow.getNextSetOfPullRequests();
				}
				return CollectionUtils.isNotEmpty(pullRequests);
			} catch (GithubConnectorException e) {
				githubRestRequester.handleInterruptedException(e);
				return false;
			}

		}

		@Override
		public EntityPrimaryDetailsCarrier next() {
			try {
				GithubPullRequestSummary pullRequest = pullRequests.remove(0);
				EntityPrimaryDetailsCarrier entityDetailsCarrier = new EntityPrimaryDetailsCarrier();

				entityDetailsCarrier.setEntityId(pullRequest.getNumber());
				entityDetailsCarrier.setEntityScopeId(getScopeForEntity(pullRequest.getNumber()));
				entityDetailsCarrier.setEntityType(connectorService.getGithubConnectorContext().getEntityType());
				entityDetailsCarrier.setProjectId(connectorService.getGithubConnectorContext().getRepositoryId());
				return entityDetailsCarrier;
			} catch (GithubConnectorException e) {
				throw new OIMRunTimeException("Problem while getting scope id for pull request", e);
			}

		}

	}

	/*
	 * Github pull request event iterator will take care of window object. The
	 * algorithm goes as follows, By default, we are creating a window in
	 * between after time and max time. It creates a window of given window
	 * size.
	 * 
	 * 1. First, hasNext of iterator is called.
	 * 
	 * 2. It checks whether the pull request list is empty and whether windowing
	 * is left. If yes, it fetches the pull request (windowing is done
	 * internally).
	 * 
	 * 3. It gets the first page of github pull requests within a window and
	 * checks whether that result is incomplete or not.
	 * 
	 * 4. If yes, then, it decreases the window size by half and gets the
	 * results again between after time and window max time till it gets a set
	 * of complete result.
	 * 
	 * 5. If no, it gets the result of other pages and adds it in list and
	 * returns.
	 * 
	 * 6. In case, after time and window time gets same, then poller will halt.
	 */
	private class GithubPullRequestEventIterator implements Iterator<NonHistoryBasedInfoCarrier> {

		private List<GithubPullRequestSummary> pullRequests;
		private final GithubPullRequestWindow pullRequestWindow;
		private final String lastProcessedId;
		private final Calendar afterTime;

		public GithubPullRequestEventIterator(final GithubPullRequestWindow pullRequestWindow,
				final String lastProcessedId, final Calendar afterTime) {
			this.pullRequestWindow = pullRequestWindow;
			pullRequests = Collections.emptyList();
			this.lastProcessedId = lastProcessedId;
			this.afterTime = afterTime;
			// Gets pull requests until and unless pull requests is empty
			// and windowing is left. This takes care of a scenario, where,
			// we don't get a pull requests in one window, then we fetch the
			// pull requests from subsequent window until and unless, we get
			// some result to poll. This is done so that core doesn't need
			// to wait for a new cycle to get another window result, in case
			// nothing is found to poll
			initializePullRequests();
			removeAlreadyProcessedPullRequests();
		}

		@Override
		public boolean hasNext() {
			return CollectionUtils.isNotEmpty(pullRequests);
		}

		@Override
		public NonHistoryBasedInfoCarrier next() {
			// It removes the pull request from the list and sends it to the
			// core for processing.
			GithubPullRequestSummary githubPullRequest = pullRequests.remove(0);
			NonHistoryBasedInfoCarrier eventCarrier;
			try {
				boolean isCreateEvent = githubPullRequest.getUpdatedAt() == null
						|| githubPullRequest.getCreatedAt().equals(githubPullRequest.getUpdatedAt());
				eventCarrier = new NonHistoryBasedInfoCarrier(githubPullRequest.getNumber(),
						connectorService.getGithubConnectorContext().getEntityType(),
						getScopeForEntity(githubPullRequest.getNumber()), new UserMeta(),
						isCreateEvent ? PollingEventType.CREATE : PollingEventType.UPDATE);
				eventCarrier.setEntityCreationTime(githubPullRequest.getCreatedAt());
				eventCarrier.setCreateUpdateTime(githubPullRequest.getUpdatedAt());
				// Getting entity object for pull request. Search API returns a
				// summary of pull requests and not details
				GithubPullRequest pullRequestEntityObj = (GithubPullRequest) getEntityObject(
						githubPullRequest.getNumber());
				if (pullRequestEntityObj != null) {
					eventCarrier.setExtraInfo(pullRequestEntityObj.constructPullRequestPropertiesMap(
							connectorService.getGithubConnectorContext().getRegExpression(), UserMeta.class));
				} else {
					LOGGER.warn("Pull request entity object is found null for id " + githubPullRequest.getNumber()
							+ " and project " + getScopeForEntity(githubPullRequest.getNumber()));
				}
				return eventCarrier;
			} catch (GithubConnectorException e) {
				throw new OIMRunTimeException("Problem while getting scope id for pull request", e);
			}

		}

		@Override
		public void remove() {
			AutomationDataHandler handler = AutomationDataHandler.getInstance(false);
			if (handler.isAutomation() && CollectionUtils.isNotEmpty(pullRequests)) {
				pullRequests.remove(0);
			} else {
				Iterator.super.remove();
			}
		}

		private void initializePullRequests() {
			LOGGER.debug("Initializing pull requests");
			try {
				while (pullRequests.isEmpty() && pullRequestWindow.isWindowingLeft()) {
					LOGGER.debug("Pull requests is empty. Hence, fetching from next window");
					pullRequests = pullRequestWindow.getNextSetOfPullRequests();
				}
			} catch (GithubConnectorException e) {
				githubRestRequester.handleInterruptedException(e);
			}
		}

		private void removeAlreadyProcessedPullRequests() {
			// Sort the elements on the basis of pull request number.
			pullRequests.sort(new PullRequestIdComparator());
			// Remove elements from list whose updated time is same as after
			// time and id is less that last processed number, so that, the
			// event is not processed again.
			pullRequests.removeIf(pullRequest -> pullRequest.getUpdatedAt().equals(afterTime)
					&& Integer.valueOf(pullRequest.getNumber()) <= Integer.valueOf(lastProcessedId));
		}

	}

	private class GithubPullRequestWindow {
		private final Calendar afterTime = Calendar.getInstance();
		private final Calendar maxTime = Calendar.getInstance();
		private final Calendar windowMaxTime = Calendar.getInstance();
		private final int windowSizeInDays;
		private boolean isInCompleteResult;

		private final String criteriaQuery;

		public GithubPullRequestWindow(final Calendar afterTime, final Calendar maxTime, final int noOfDays,
				final String criteriaQuery) {
			this.afterTime.setTimeInMillis(afterTime.getTimeInMillis());
			this.maxTime.setTimeInMillis(maxTime.getTimeInMillis());
			windowMaxTime.setTimeInMillis(afterTime.getTimeInMillis());
			windowSizeInDays = noOfDays;
			isInCompleteResult = false;
			this.criteriaQuery = criteriaQuery;
		}

		/*
		 * This fetches the pull requests in between after time and window max
		 * time, with pagination. This function updates both aftertime and
		 * window maxtime.
		 * 
		 * It updates the window maxtime to aftertime + window size to fetch
		 * data between a window.
		 * 
		 * If empty data is received in the window, then we update the window
		 * maxtime to maxtime and get the first updated entity in that window.
		 * 
		 * If an entity is found then, we update the aftertime to found entity
		 * updated time and fetch the data between aftertime and window maxtime
		 * 
		 * If entity is not found, then we set aftertime to window maxtime,
		 * because that window is already processed and we don't have any data
		 * in that window.
		 * 
		 * Then, if any windowing is left, then processing continues till we get
		 * an entity to process or window maxtime reaches maxtime i.e. windowing
		 * is completed.
		 */
		public List<GithubPullRequestSummary> getNextSetOfPullRequests() throws GithubConnectorException {
			List<GithubPullRequestSummary> listOfPullRequests = new ArrayList<>();
			// update window max time to current + number of days, only when
			// incomplete result in false. Because, in case incomplete result is
			// false.
			updateWindowMaxTime();
			LOGGER.debug("Created window to fetch pull requests updated in between start time: "
					+ afterTime.getTime().toString() + " and finish time: " + windowMaxTime.getTime().toString());
			// Get pull request in window of after time and window max time
			final GithubPullRequests pullRequests = getPullRequestsInWindow(GithubConstants.DEFAULT_PAGE_SIZE);
			listOfPullRequests.addAll(pullRequests.getPullRequests());
			if (pullRequests.isSecondPageAvailable()) {
				listOfPullRequests.addAll(
						githubRestRequester.getPullRequestsInBetween(afterTime, windowMaxTime, criteriaQuery, false,
								GithubConstants.DEFAULT_PAGE_SIZE).getPullRequests());
			}
			// updating after time to window max time to start next window
			// from window max time.
			LOGGER.debug("Pull Requests Found: " + listOfPullRequests.size());
			return checkForEmptyWindowBetweenAfterTimeMaxTime(listOfPullRequests);
		}

		public boolean isWindowingLeft() {
			// Checking if window max time is less than max time. If
			// yes, then windowing is left. If window max time is equal to max
			// time, then no windowing is left. All the pull requests has been
			// processed till max time
			return windowMaxTime.compareTo(maxTime) < 0;
		}

		/*
		 * Incomplete result handling - This code just handles incomplete
		 * results from Github. When incomplete result is found, it divides the
		 * window in half and sends request again with that window.
		 * 
		 * Here, we are just updating window maxtime.(after time is not updated
		 * here)
		 */
		private GithubPullRequests getPullRequestsInWindow(final int pageSize) throws GithubConnectorException {
			// Get pull requests in between after time and window max time.
			LOGGER.debug("Getting Pull Request for start time: " + afterTime.getTime().toString()
					+ " and finish time: " + windowMaxTime.getTime().toString());
			GithubPullRequests pullRequestsData = githubRestRequester
					.getPullRequestsInBetween(afterTime, windowMaxTime, criteriaQuery, true, pageSize);
			isInCompleteResult = pullRequestsData.isIncompleteResult();

			if (isInCompleteResult) {
				LOGGER.debug("Incomplete results found from server. Hence decreasing window size to half.");
				// Decrease window by half and call next method again.
				// return value from below function, if window time is same as
				// previous, break from recursion, throwing error
				halfWindowTime();
				return getPullRequestsInWindow(pageSize);
			} else {
				LOGGER.debug("Incomplete results not found from server. Hence returning pull requests found");
				return pullRequestsData;
			}
		}

		private List<GithubPullRequestSummary> checkForEmptyWindowBetweenAfterTimeMaxTime(
				final List<GithubPullRequestSummary> listOfPullRequests) throws GithubConnectorException {
			// update aftertime to window maxtime, because aftertime and window
			// maxtime window is processed
			afterTime.setTimeInMillis(windowMaxTime.getTimeInMillis());
			// If we didn't get any entities in between aftertime and window
			// maxtime, we will get first entity updated after window maxtime
			// and before maxtime.

			if (CollectionUtils.isEmpty(listOfPullRequests)) {
				// When pull request list is empty, we get the pull request
				// which was first created/updated after the aftertime and
				// before max time. This helps
				// us save the time for windowing.
				// Setting up window maxtime to maxtime and fetching the first
				// updated pull request in between aftertime and window maxtime
				windowMaxTime.setTimeInMillis(maxTime.getTimeInMillis());
				GithubPullRequests firstCreatedPullRequestTillMaxTime = getPullRequestsInWindow(1);
				List<GithubPullRequestSummary> pullRequestSummary = firstCreatedPullRequestTillMaxTime.getPullRequests();
				if (CollectionUtils.isNotEmpty(pullRequestSummary)) {
					// Update aftertime and window max time according to the
					// updated time of entity received
					updateAfterTimeWindowTimeToFirstCreatedPR(pullRequestSummary.get(0).getUpdatedAt());
					return getNextSetOfPullRequests();
				} else {
					// If no entity found, then update aftertime to window
					// maxtime. Windowmax time may be changd by call to getPullRequestsInWindow, hence updating aftertime again.
					afterTime.setTimeInMillis(windowMaxTime.getTimeInMillis());
				}
			}
			return listOfPullRequests;
		}

		private void updateAfterTimeWindowTimeToFirstCreatedPR(final Calendar firstPRCreatedUpdatedTime) {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(firstPRCreatedUpdatedTime.getTimeInMillis());
			if (cal.compareTo(maxTime) < 1) {
				afterTime.setTimeInMillis(firstPRCreatedUpdatedTime.getTimeInMillis());
				windowMaxTime.setTimeInMillis(firstPRCreatedUpdatedTime.getTimeInMillis());
			} else {
				afterTime.setTimeInMillis(maxTime.getTimeInMillis());
				windowMaxTime.setTimeInMillis(maxTime.getTimeInMillis());
			}
		}

		// This function adds window days in window max time
		private void updateWindowMaxTime() {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(windowMaxTime.getTimeInMillis());
			cal.add(Calendar.DATE, windowSizeInDays);
			if (cal.compareTo(maxTime) < 1) {
				windowMaxTime.add(Calendar.DATE, windowSizeInDays);
			}else {
				windowMaxTime.setTimeInMillis(maxTime.getTimeInMillis());
			}
		}

		// this function makes window max time to half
		private void halfWindowTime() {
			int daysBetween = (int) ChronoUnit.DAYS.between(afterTime.toInstant(), windowMaxTime.toInstant());
			if (daysBetween <= 1) {
				throw new OIMRunTimeException(
						"The window size is one day. Github server is responding with incomplete results. Hence, halting the poller for some time. Retry again later.",
						null);
			} else {
				windowMaxTime.add(Calendar.DATE, daysBetween / 2 * -1);
			}
		}

	}

	private class PullRequestIdComparator implements Comparator<GithubPullRequestSummary> {
		@Override
		public int compare(final GithubPullRequestSummary pullRequest1, final GithubPullRequestSummary pullRequest2) {
			// this will sort the elements whose updated time are same on the
			// basis of pull request number
			if (pullRequest1 == null || pullRequest1.getUpdatedAt() == null)
				return -1;
			if (pullRequest2 == null || pullRequest2.getUpdatedAt() == null)
				return 1;
			if (pullRequest1.getUpdatedAt().equals(pullRequest2.getUpdatedAt())) {
				return Integer.valueOf(pullRequest1.getNumber()).compareTo(Integer.valueOf(pullRequest2.getNumber()));
			}else{
				return pullRequest1.getUpdatedAt().compareTo(pullRequest2.getUpdatedAt());
			}
		}

	}

	@Override
	public Map<String, Object> getCurrentState(final ProcessingCarrier entityDetails) throws OIMGithubPollerException {
		if (MapUtils.isNotEmpty(entityDetails.getExtraInfo())) {
			return entityDetails.getExtraInfo();
		}
		LOGGER.warn("Github Pull Request current state found empty for entity id " + entityDetails.getInternalId()
				+ " and project id " + entityDetails.getProjectKey());
		return new HashMap<>();
	}

	@Override
	public String getRemoteEntityLink(final String id) throws GithubConnectorException {
		GithubPullRequest pullRequest = (GithubPullRequest) getEntityObject(id);
		if (pullRequest != null) {
			return pullRequest.getHtmlUrl();
		}
		LOGGER.warn("Pull request found null. Hence, setting remote entity links as empty");
		return "";
	}

	private EAIEntityRefrences processLinks(final List<Commit> commits, final String linkType)
			throws GithubConnectorException {
		String scopeId = getScopeForEntity("");
		List<EAILinkEntityItem> linkedEntityItems = commits != null ? commits.stream()
					.map(commit -> new EAILinkEntityItem(GithubEntityFactory.GitALMEntityType.CHANGE_SET.getEntityType(),
							commit.getSha(), scopeId, commit.getCommit().getCommitter().getName()))
				.collect(Collectors.toList()) : Collections.emptyList();
			return new EAIEntityRefrences(linkType, linkedEntityItems);
		}

	@Override
	public EAIEntityRefrences getCurrentLinks(final String internalId, final String linkType)
			throws GithubConnectorException {
		UrlBuilder urlBuilder = GithubUrlBuilderHandler
				.getURLForCommitsInPullRequest(githubRestRequester.getRepoNameFromId(), internalId);
		List<Commit> commits = githubRestRequester.sendReadRequestWithPagination(urlBuilder, Commit.class);
		return processLinks(commits, linkType);
	}

	@Override
	public List<EAIEntityRefrences> getLink(final NonHistoryBasedInfoCarrier entityDetails, final Object currentState)
			throws OIMGithubPollerException {
		List<EAIEntityRefrences> eaiEntityReferences = new ArrayList<>();
		try {
			List<String> linkTypes = Arrays.asList(GithubConstants.GithubLinkTypes.COMMITS_LINK);
			for (String linkType : linkTypes) {
				LOGGER.debug("Fetching links of type : " + linkType);
				eaiEntityReferences.add(getCurrentLinks(entityDetails.getInternalId(), linkType));
			}
		} catch (GithubConnectorException e) {
			throw new OIMGithubPollerException(GithubConstants.ErrorCode._017433,
					new String[] { entityDetails.getInternalId(), e.getMessage() }, e);
		}
		return eaiEntityReferences;
	}

	private String getScopeForEntity(final String internalId) throws GithubConnectorException {
		String scopeId;
		try {
			scopeId = connectorService.getScopeId(internalId, null, null);
		} catch (OIMAdapterException e) {
			throw new GithubConnectorException(e);
		}
		return scopeId;
	}




	@Override
	public ExtendedCommentsMeta getExtendedCommentsMeta() {
		return new ExtendedCommentsMeta(getCommentsMeta(), false, true);
	}



	@Override
	public List<EAIComment> getComments(final String entityID, final String afterCommentID, final Calendar afterTime)
			throws GithubConnectorException {
		List<GitHubComment> issueComments = getCommentsOfType(entityID, afterTime,
				CommentTypeRequestParam.ISSUE_COMMENT);
		List<GitHubComment> reviewComments = getCommentsOfType(entityID, afterTime,
				CommentTypeRequestParam.REVIEW_COMMENT);

		List<EAIComment> eaiComments = new ArrayList<EAIComment>();
		if (CollectionUtils.isNotEmpty(issueComments)) {
			eaiComments.addAll(issueComments.stream()
					.map(x -> x.toEaiComment(CommentTypeRequestParam.ISSUE_COMMENT.getCommentType()))
					.collect(Collectors.toList()));
		}
		if (CollectionUtils.isNotEmpty(reviewComments)) {
			eaiComments.addAll(reviewComments.stream()
					.map(x -> x.toEaiComment(CommentTypeRequestParam.REVIEW_COMMENT.getCommentType()))
					.collect(Collectors.toList()));
		}
		Collections.sort(eaiComments, new EAICommentComparator());
		
		return eaiComments;
		

	}

	/**
	 * @param entityID
	 * @param afterTime
	 * @param commentTypeParam
	 * @throws GithubConnectorException
	 */
	private List<GitHubComment> getCommentsOfType(final String entityID, final Calendar afterTime,
			final CommentTypeRequestParam commentTypeParam)
			throws GithubConnectorException {
		UrlBuilder urlBuilder = GithubUrlBuilderHandler.getURLForGivenTypeCommentsOfPullRequest(
				githubRestRequester.getRepoNameFromId(), entityID, afterTime, commentTypeParam);
		return githubRestRequester.sendReadRequestWithPagination(urlBuilder,
				GitHubComment.class);
	}

	/**
	 * Returning attachment meta for only source as true as inline image and
	 * document will get polled in case of Github pull request entity
	 */
	@Override
	public AttachmentMeta getAttachementMeta() {
		AttachmentMeta attachmentMeta = new AttachmentMeta();
		attachmentMeta.setAsSourceSupported(true);
		attachmentMeta.setAsTargetSupported(false);
		attachmentMeta.setDeleteSupported(false);
		attachmentMeta.setHistorySupported(false);
		attachmentMeta.setUpdateSupported(false);
		return attachmentMeta;
	}

	@Override
	public InputStream getAttachmentInputStream(final String attachmentURI) throws OIMAdapterException {
		return githubRestRequester.getInputStreamFromURL(attachmentURI);
	}

	@SuppressWarnings("unchecked")
	@Override
	public String getCreatedBy(final ProcessingCarrier entityDetails, final Object entityCurrentState,
			final String entityID) throws EAIPollerException {
		// Return the user who created PR in github
		try {
			if (entityCurrentState != null) {
				UserMeta userMeta = (UserMeta) ((HashMap<String, Object>) entityCurrentState).get(CREATED_USER);
				return userMeta != null ? userMeta.getUserName() : null;
			} else {
				GithubPullRequest pullRequest = (GithubPullRequest) getEntityObject(entityID);
				if (pullRequest != null) {
					return pullRequest.getCreatedBy().getLoginId();
				}
				return null;
			}
		} catch (GithubConnectorException e) {
			throw new EAIPollerException(GithubConstants.ErrorCode._017431,
					new String[] { entityDetails.getInternalId() }, e);
		}
	}

}
