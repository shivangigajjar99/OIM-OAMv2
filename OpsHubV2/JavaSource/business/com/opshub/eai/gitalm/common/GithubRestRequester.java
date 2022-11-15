package com.opshub.eai.gitalm.common;

import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.opshub.eai.gitalm.adapter.GithubConnectorContext;
import com.opshub.eai.gitalm.common.GithubConstants.UserType;
import com.opshub.eai.gitalm.data.Branches;
import com.opshub.eai.gitalm.data.Commit;
import com.opshub.eai.gitalm.data.File;
import com.opshub.eai.gitalm.data.GithubLabel;
import com.opshub.eai.gitalm.data.GithubPullRequestSummary;
import com.opshub.eai.gitalm.data.GithubPullRequests;
import com.opshub.eai.gitalm.data.GithubTeam;
import com.opshub.eai.gitalm.data.GithubUser;
import com.opshub.eai.gitalm.exceptions.GithubConnectorException;
import com.opshub.eai.gitalm.exceptions.GithubConnectorInterruptedException;
import com.opshub.eai.gitalm.exceptions.GithubEntityObjectNotFoundException;
import com.opshub.eai.gitalm.metadata.impl.GithubPullRequestFieldsMeta;
import com.opshub.eai.metadata.FieldLookup;
import com.opshub.eai.metadata.ProjectMeta;
import com.opshub.eai.reqhandler.CommonCrudRequester;
import com.opshub.eai.reqhandler.HttpDefaultConstants.MethodType;
import com.opshub.eai.reqhandler.builder.QueryBuilder;
import com.opshub.eai.reqhandler.builder.UrlBuilder;
import com.opshub.eai.reqhandler.data.APIKeyAuthBuilder;
import com.opshub.eai.reqhandler.data.CRUDResponseObject;
import com.opshub.eai.reqhandler.data.NameValuePair;
import com.opshub.exceptions.CRUDRequesterException;
import com.opshub.exceptions.eai.OIMRunTimeException;
import com.opshub.logging.OpsHubLoggingUtil;
import com.opshub.scmclients.FileLogMessageBean;
import com.opshub.scmclients.LogMessageBean;
import com.opshub.scmclients.Operation;
import com.opshub.scmclients.SCMSystemsConstants;

public class GithubRestRequester {

	private final GithubConnectorContext connectorContext;

	private static final Logger LOGGER = Logger.getLogger(GithubRestRequester.class);
	private final String restUrl;
	private final Map<String, GithubRepo> repoDetailsList;
	private CommonCrudRequester crudRequester;
	private static final String PER_PAGE_STRING = "per_page";
	private static final String PAGE_STRING = "page";
	private static final String PER_PAGE = "100";
	private HashMap<String, String> cachedFileContent = new HashMap<String, String>();

	public static final String DEFAULTISSUEID = "-1";

	private boolean retryJobInterrupted;

	public GithubRestRequester(final GithubConnectorContext connectorContext) throws GithubConnectorException {
		this.connectorContext = connectorContext;
		restUrl = connectorContext.getBaseURL();
			APIKeyAuthBuilder apiKeyAuthBuilder = new APIKeyAuthBuilder("Authorization",
					"token " + connectorContext.getPersonalAccessToken());
			crudRequester = CommonCrudRequester.newBuilder().apiKeyAuthentication(apiKeyAuthBuilder, restUrl)
					.build();
		List<GithubRepo> repositories = getAllRepositories();
		repoDetailsList = processRepositories(repositories);
	}

	private Map<String,GithubRepo> processRepositories(final List<GithubRepo> repositories){
		return repositories != null
				? repositories.stream().collect(Collectors.toMap(GithubRepo::getId, repository -> repository))
				: Collections.emptyMap();
	}

	/**
	 * @param commentMessage:
	 *            the message in which the regex is to be found out
	 * @param regex:
	 *            the regex which is to be found in the commentMessage [A-Z]
	 * @return
	 * @throws GithubConnectorException
	 */
	public static String getTargetEntityIdByFindingRegex(final String commentMessage, final String regex)
			throws GithubConnectorException {
		String issueId = GithubRestRequester.DEFAULTISSUEID;
		if (StringUtils.isBlank(regex)) {
			LOGGER.error("Regex found : "+regex);
			throw new GithubConnectorException("00055", new String[] {}, null);
		}
		LOGGER.debug("Parsing message : " + commentMessage + " using Regex : " + regex);
		if (commentMessage != null && !commentMessage.equals("")) {
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(commentMessage);
			if (matcher.find()) {
				String expressionMatched = matcher.group();
				if (!"".equals(expressionMatched)){
					issueId = expressionMatched;
				}
			}
		}
		LOGGER.debug("Issue id found : " + issueId);
		return issueId;
	}


	/**
	 * @param commit
	 * @return
	 * @throws GithubConnectorException
	 */
	public LogMessageBean getLogMessagebean(final Commit commit) throws GithubConnectorException {
		LogMessageBean logMessage = new LogMessageBean();
		logMessage.setComment(commit.getCommit().getMessage());
		logMessage.setCommitId(commit.getSha());
		logMessage.setUserName(commit.getCommit().getAuthor().getName());
		logMessage.setUserEmail(commit.getCommit().getAuthor().getEmail());

		logMessage.setAuthor(commit.getCommit().getCommitter().getName());
		logMessage.setAuthorEmail(commit.getCommit().getCommitter().getEmail());
		logMessage.setTime(new Timestamp(commit.getCommitedDate().getTimeInMillis()));

		FileLogMessageBean[] fileLogBeanArray = getFileLogMessageBean(commit.getFiles());
		logMessage.setFileLogMessageBeans(fileLogBeanArray);
		return logMessage;

	}

	/**
	 * This method returns list of file log messagebean from list of files
	 * 
	 * @param files
	 * @return
	 * @throws GithubConnectorException
	 */
	private FileLogMessageBean[] getFileLogMessageBean(final List<File> files) throws GithubConnectorException {
		if (CollectionUtils.isNotEmpty(files)) {
			FileLogMessageBean[] fileLogBeanArray = new FileLogMessageBean[files.size()];
			for (int i = 0; i < files.size(); i++) {
				fileLogBeanArray[i] = getFileLogMessageBean(files.get(i));
			}
			return fileLogBeanArray;
		}
		return new FileLogMessageBean[0];
	}

	/**
	 * This method returns File Log messagebean from one file
	 * 
	 * @param files
	 * @return
	 * @throws GithubConnectorException
	 */
	private FileLogMessageBean getFileLogMessageBean(final File files) throws GithubConnectorException {

		FileLogMessageBean fileLogMsgBean = new FileLogMessageBean();
		if (files != null) {
			fileLogMsgBean.setFileName(files.getFilename());
			fileLogMsgBean.setFileOperationType(getFileOpertaionType(files.getStatus()));
			fileLogMsgBean.setFileOperationTypeMessage(files.getStatus());
			fileLogMsgBean.setCurRevisionNo(files.getSha());
			fileLogMsgBean.setFileContentPath(files.getContents_url());
			if (files.getStatus().equals(Operation.renamed.toString()))
				fileLogMsgBean.setPreviousFileContentPath(files.getPrevious_filename());
			else
				fileLogMsgBean.setPreviousFileContentPath(files.getFilename());
		}
		return fileLogMsgBean;
	}

	/**
	 * @param status
	 * @return
	 */
	private int getFileOpertaionType(final String status) {

		int retType = -1;
		if (GithubConstants.GitALMJsonKeys.REMOVED.equals(status)) {
			retType = SCMSystemsConstants.DELETED;
		} else if (GithubConstants.GitALMJsonKeys.ADDED.equals(status)) {
			retType = SCMSystemsConstants.ADDED;
		} else if (GithubConstants.GitALMJsonKeys.RENAMED.equals(status)) {
			retType = SCMSystemsConstants.RENAMED;
		} else {
			retType = SCMSystemsConstants.MODIFIED;
		}
		return retType;
	}

	public Commit getCommitInformation(final String reponame, final String commitId) throws GithubConnectorException
		{
		try {
			return (Commit) getData(false, new TypeReference<Commit>() {
			}, null, GithubConstants.RestRequesterConstants.REPOSITORY, reponame,
					GithubConstants.RestRequesterConstants.COMMITS, commitId);
		} catch (GithubConnectorException e) {
			//If commit information is not passed for any commit sha, the error is logged and null is returned
			//This is done to avoid running into deadlock for not getting commit information for any individual commit
			String errorMessage = "\"message\":\"No commit found for SHA: " + commitId;
			if(e.getMessage().contains(errorMessage)) {
				OpsHubLoggingUtil.error(LOGGER, "Commit information for the commit " + commitId + " cannot be retrieved.", e);
				return null;
			} 
			else {
				throw e;
			}
		}
	}

	/**
	 * Return all cahngeset by given branch and provided start and end time if
	 * star time and end time can be empty
	 * 
	 * @param reponame
	 *            Repository Name
	 * @param branchName
	 *            Branch Name
	 * @param startTime
	 *            Start Timne - can be empty
	 * @param endTime
	 *            End Time - can be empty
	 * @return
	 * @throws GithubConnectorException
	 */
	public List<Commit> getAllChangset(final String reponame, final String branchName, final Calendar startTime,
			final Calendar endTime) throws GithubConnectorException {
		HashMap<String, String> whereClause = new HashMap<>();
		whereClause.put(GithubConstants.RestRequesterConstants.SHA, branchName);
		if (startTime != null) {
			String startTimeInStr = GithubUtility.getDateStringFromCalendar(startTime);
			whereClause.put(GithubConstants.RestRequesterConstants.SINCE, startTimeInStr);
		}
		if (endTime != null) {
			String endTimeInStr = GithubUtility.getDateStringFromCalendar(endTime);
			whereClause.put(GithubConstants.RestRequesterConstants.UNTIL, endTimeInStr);
		}

		List<Commit> allGitCommits = (List<Commit>) getData(true, new TypeReference<List<Commit>>() {
		}, whereClause, GithubConstants.RestRequesterConstants.REPOSITORY, reponame,
				GithubConstants.RestRequesterConstants.COMMITS);
		if (CollectionUtils.isNotEmpty(allGitCommits)) {
			for (Commit commit : allGitCommits) {
				commit.setBranchName(branchName);
				commit.setPushTime(commit.getCommitedDate());
			}
			return allGitCommits;
		}
		return Collections.emptyList();

	}

	/**
	 * Returns all repositories
	 * 
	 * @return
	 * @throws GithubConnectorException
	 */
	public List<ProjectMeta> getAllProjects() throws GithubConnectorException {
		List<ProjectMeta> projectList = new ArrayList<ProjectMeta>();
		if (repoDetailsList != null && !repoDetailsList.isEmpty()) {
			// converting gitRepoList into project ProjectsMeta List
			for (GithubRepo gitRepo : repoDetailsList.values()) {
				projectList.add(new ProjectMeta(gitRepo.getId(), "/" + gitRepo.getFullName()));
			}
		} else {
			OpsHubLoggingUtil.debug(LOGGER, "No repositories found for this github instance", null);
		}
		return projectList;
	}

	/**
	 * This method returns branch name given by repository name
	 * 
	 * @param repoName
	 *            full repo name example - username/main
	 * @return List of branches
	 * @throws GithubConnectorException
	 */
	public List<Branches> getAllBranches(final String repoName) throws GithubConnectorException {

		return (List<Branches>) getData(true, new TypeReference<List<Branches>>() {
		}, null, GithubConstants.RestRequesterConstants.REPOSITORY, repoName,
				GithubConstants.RestRequesterConstants.BRANCHES);

	}

	/**
	 * Gives all repositories present in the instance
	 * 
	 * @return
	 * @throws GithubConnectorException
	 */
	public List<GithubRepo> getAllRepositories() throws GithubConnectorException {
		return (List<GithubRepo>) getData(true, new TypeReference<List<GithubRepo>>() {
		}, null, GithubConstants.RestRequesterConstants.USER, GithubConstants.RestRequesterConstants.REPOSITORY);

	}

	/**
	 * This method is used for geting response in list of give type reference,
	 * it will iterate data in page wise as well
	 * 
	 * @param typeReference
	 * @param parts
	 * @return
	 * @throws GithubConnectorException
	 */
	@SuppressWarnings("unchecked")
	private Object getData(final boolean isPaginated, final TypeReference typeReference,
			final HashMap<String, String> whereClause, final String... parts)
			throws GithubConnectorException {
		QueryBuilder queryBuilder = null;
		List<Object> allData = new ArrayList<>();
		try {
			CRUDResponseObject crudResponseObject = null;
			List<Object> data = null;
			int pageNo = 1;
			do {
				UrlBuilder urlBuilder = UrlBuilder.newUrlBuilder();
				queryBuilder = QueryBuilder.newQueryBuilder();
				queryBuilder.genericClause(PER_PAGE_STRING, PER_PAGE);
				queryBuilder.genericClause(PAGE_STRING, String.valueOf(pageNo));
				if (whereClause != null) {
					Iterator<Entry<String, String>> itrOnWhereClause = whereClause.entrySet().iterator();
					while (itrOnWhereClause.hasNext()) {
						Entry<String, String> entry = itrOnWhereClause.next();
						queryBuilder.genericClause(entry.getKey(), entry.getValue());
					}
				}
				pageNo++;
				List<String> listOfParts = new ArrayList<String>();
				for (String part : parts) {
					if (part != null && part.contains("/")) {
						String[] numberOfParts = part.split("/");
						listOfParts.addAll(Arrays.asList(numberOfParts));
					} else {
						listOfParts.add(part);
					}
				}
				urlBuilder.addUrlParts(listOfParts.toArray(new String[0]));
				urlBuilder.addQueryParts(queryBuilder);
				crudResponseObject = sendReadRequest(urlBuilder);

				if (!isPaginated) {
					return GithubUtility.convertToObject(crudResponseObject.getResponseAsString(), typeReference);
				}
				data = (List<Object>) GithubUtility.convertToObject(crudResponseObject.getResponseAsString(),
						typeReference);
				data = data != null ? data : Collections.emptyList();
				allData.addAll(data);
			} while (data.size() == Integer.parseInt(PER_PAGE));
		} catch (CRUDRequesterException e) {
			OpsHubLoggingUtil.error(LOGGER, "Error while sending the to fetch data", e);
			throw new GithubConnectorException(GithubConstants.ErrorCode._00035, new String[] {}, e);
		}
		return allData;
	}


	/**
	 * Returns all values of lookups
	 * 
	 * @param fieldName
	 * @return
	 * @throws GithubConnectorException
	 */
	public List<FieldLookup> getLookups(final String fieldName) throws GithubConnectorException {
		if (fieldName.equals(GithubConstants.GitALMFieldInternalNames.STATE)) {
			return getLookupForState();
		} else if (GithubConstants.GitALMFieldInternalNames.LABELS.equals(fieldName)
				|| GithubPullRequestFieldsMeta.PullRequestRestFields.MultiValuedLookUpFields.LABELS.equals(fieldName)) {
			return getLookupValuesForLabels();
		} else if (GithubPullRequestFieldsMeta.PullRequestRestFields.MultiValuedLookUpFields.TEAM_NAME
				.equals(fieldName)) {
			return getLookupValuesForTeams();
		}else {
			OpsHubLoggingUtil.error(LOGGER, "Field lookups don't exist for field" + fieldName, null);
			throw new GithubConnectorException(GithubConstants.ErrorCode._0003,
					new String[] { "Fetching lookups", fieldName }, null);
		}
	}


	private List<FieldLookup> getLookupForState() {
		List<FieldLookup> fieldLookups = new ArrayList<>();
		fieldLookups.add(new FieldLookup(GithubConstants.GitALMFieldNames.STATE_OPEN,
				GithubConstants.GitALMFieldInternalNames.STATE_OPEN));
		fieldLookups.add(new FieldLookup(GithubConstants.GitALMFieldNames.STATE_CLOSED,
				GithubConstants.GitALMFieldInternalNames.STATE_CLOSED));
		return fieldLookups;
	}

	private List<FieldLookup> getLookupValuesForLabels() throws GithubConnectorException {
		UrlBuilder urlBuilder = GithubUrlBuilderHandler.getURLForLabels(getRepoNameFromId());
		List<GithubLabel> githubLabels = sendReadRequestWithPagination(urlBuilder, GithubLabel.class);
		return githubLabels.stream().map(label -> new FieldLookup(label.getName(), label.getName()))
				.collect(Collectors.toList());
	}

	private List<FieldLookup> getLookupValuesForTeams() throws GithubConnectorException {
		GithubRepo repository = repoDetailsList.get(connectorContext.getRepositoryId());
		if (UserType.Organization.equals(repository.getOwnerType())) {
			LOGGER.debug("Fetching lookup for teams in repository - " + repository.getName()
					+ " because the owner of repository is an Organization");
			UrlBuilder urlBuilder = GithubUrlBuilderHandler.getURLForTeams(repository);
			List<GithubTeam> githubTeams = sendReadRequestWithPagination(urlBuilder, GithubTeam.class);
			return githubTeams.stream().map(team -> new FieldLookup(team.getName(), team.getSlug()))
					.collect(Collectors.toList());
		}
		LOGGER.debug("Setting empty lookup for teams in repository - " + repository.getName()
				+ " because the owner of repository is not an Organization");
		return Collections.emptyList();
	}

	/**
	 * Returns repository name for given repoId
	 * 
	 * @return
	 */
	public String getRepoNameFromId() {
		return repoDetailsList.get(connectorContext.getRepositoryId()) != null
				? repoDetailsList.get(connectorContext.getRepositoryId()).getFullName()
				: "";
	}


	public <T> T sendReadRequestWithoutPagination(final UrlBuilder urlBuilder, final TypeReference<T> className)
			throws GithubConnectorException {
		try {
			CRUDResponseObject crudResponseObject = sendReadRequest(urlBuilder);
			return crudResponseObject.getJsonResponseAs(className, true);
		} catch (CRUDRequesterException ex) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00052,
					new String[] { urlBuilder.getUrlBuilt(), ex.getMessage() }, ex);
		}
	}

	/*
	 * This method is to be used only when list is small. Otherwise, it will
	 * load all data into one complete list
	 */
	public <T> List<T> sendReadRequestWithPagination(final UrlBuilder urlBuilder, final Class<T> className)
			throws GithubConnectorException {
		List<T> resultList = new ArrayList<T>();
		boolean isPaginatedRequest = false;
		try {
			do {
				CRUDResponseObject crudResponseObject = sendReadRequest(urlBuilder);
				resultList.addAll(crudResponseObject.getJsonResponseAsListOf(className, true));
				isPaginatedRequest = checkResponseHeadersForNextPage(crudResponseObject);
				if (isPaginatedRequest) {
					urlBuilder.nextPage();
				}
			} while (isPaginatedRequest);
		} catch (CRUDRequesterException ex) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00052,
					new String[] { urlBuilder.getUrlBuilt(), ex.getMessage() }, ex);
		}
		return resultList;
	}

	/*
	 * This method checks the status code. If 404, then throws error with error
	 * code 00053. Means response is not correct. If status code 200 or 201 or
	 * 204, then response is correct then return directly. If other than that,
	 * then response is not correct and throws error with status code 00051.
	 * 
	 */
	private boolean checkStatusCodeAndRetry(final CRUDResponseObject response, final String urlBuilt)
			throws GithubConnectorException {
		int statusCode = response.getStatusCode();
		try {
			if(statusCode == 403) {
				LOGGER.error("Forbidden code received due to " + response.getStatusReasonPhrase());
				String responseString = response.getResponseAsString();
				responseString = responseString == null ? "" : responseString.toLowerCase(Locale.US);
				if (responseString.contains(GithubConstants.GithubResponseString.API_RATE_LIMIT_EXCEEDED.toLowerCase(Locale.US))
						|| responseString.contains(GithubConstants.GithubResponseString.YOU_HAVE_TRIGGERED_AN_ABUSE_DETECTION.toLowerCase(Locale.US))) {
					return true;
				}
			}
			checkStatusCodeForError(response, urlBuilt, statusCode);
			return false;
		} catch (final CRUDRequesterException e) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00052,
					new String[] { urlBuilt, e.getMessage() }, e);
		}
	}

	/**
	 * @param response
	 * @param urlBuilt
	 * @param statusCode
	 * @throws GithubEntityObjectNotFoundException
	 * @throws CRUDRequesterException
	 * @throws GithubConnectorException
	 */
	private void checkStatusCodeForError(final CRUDResponseObject response, final String urlBuilt, final int statusCode)
			throws GithubEntityObjectNotFoundException, CRUDRequesterException, GithubConnectorException {
		if (statusCode == 404) {
			LOGGER.error("Entity not found in Github due to " + response.getStatusReasonPhrase());
			throw new GithubEntityObjectNotFoundException(GithubConstants.ErrorCode._00053,
					new String[] { urlBuilt, response.getResponseAsString() }, null);
		}
		if (!(statusCode == 200 || statusCode == 201 || statusCode == 204)) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00051,
					new String[] { urlBuilt, response.getResponseAsString() },
					null);
		}
	}
	
	
	private void checkResponseHeadersForApiRateLimit(final CRUDResponseObject response)
			throws GithubConnectorInterruptedException, InterruptedException {
		List<NameValuePair> responseHeadersList = response.getResponseHeaders() != null ? response.getResponseHeaders()
				: Collections.emptyList();
		NameValuePair rateLimitRemainingHeader = responseHeadersList.stream()
				.filter(responseHeader -> GithubConstants.GithubResponseHeader.X_RATE_LIMIT_REMAINING.equalsIgnoreCase(responseHeader.getKey())).findFirst()
				.orElse(new NameValuePair("", "1"));
		LOGGER.warn("Github Request rate limit left is : " + rateLimitRemainingHeader.getValue());
		if (Integer.parseInt(rateLimitRemainingHeader.getValue()) <= 1) {
			// Log error and wait for sometime
			NameValuePair rateLimitResetHeader = responseHeadersList.stream()
					.filter(responseHeader -> GithubConstants.GithubResponseHeader.X_RATE_LIMIT_RESET.equalsIgnoreCase(responseHeader.getKey())).findFirst()
					.orElse(new NameValuePair("", "0"));
			LOGGER.warn("Github Rate limit time reset value is " + rateLimitRemainingHeader.getValue());
			// The header has time at which the current rate limit window resets
			// in UTC epoch seconds. Converting it in milliseconds
			long resetTimeInMillis = Long.valueOf(rateLimitResetHeader.getValue()) * 1000;
			LOGGER.warn("Github Request rate limit exceeded. Rate limit will be reset at "
					+ GithubUtility.getCalendarFromMillis(resetTimeInMillis).getTime().toString());
			waitTillResetTime(resetTimeInMillis);
		}
	}

	private void waitTillResetTime(final long resetTimeInMillis)
			throws InterruptedException, GithubConnectorInterruptedException {
		boolean wait = true;
		LOGGER.warn("Total wait time : " + resetTimeInMillis * 0.001 + " seconds");
		do {
			long currentTimeInMillis = Instant.now().toEpochMilli();
			long diff = (long) ((resetTimeInMillis - currentTimeInMillis) * 0.1);
			if (diff > 0) {
				LOGGER.warn("Waiting for " + diff * 0.001 + " seconds");
				Thread.sleep(diff);
			} else {
				LOGGER.warn("Wait time for Github end system over. API Limit is reset. Resuming execution and retrying.");
				wait = false;
			}
			if (isRetryInterrupted()) {
				throw new GithubConnectorInterruptedException("00054", new String[] {}, null);
			}
		} while (wait);
	}
	
	public GithubPullRequests getPullRequestsInBetween(final Calendar afterTime, final Calendar maxTime,
			final String criteriaQuery, final boolean onlyFirstPage, final int pageSize)
			throws GithubConnectorException {
		if (onlyFirstPage) {
			return getPullRequestsInBetweenOnlyFirstPage(afterTime, maxTime, criteriaQuery,pageSize);
		} else {
			return getPullRequestsInBetweenWithPagination(afterTime, maxTime, 2, criteriaQuery);
		}
	}

	private GithubPullRequests getPullRequestsInBetweenOnlyFirstPage(final Calendar afterTime,
			final Calendar maxTime, final String criteriaQuery, final int pageSize) throws GithubConnectorException {
		UrlBuilder urlBuilder = GithubUrlBuilderHandler.getURLBuilderForGetEntitiesChanged(afterTime, maxTime, 1,
				criteriaQuery, getRepoNameFromId(), pageSize);
		return sendReadRequestWithoutPagination(urlBuilder, new TypeReference<GithubPullRequests>() {});
	}

	/*
	 * For fetching pull requests, we are using search API. Search API has
	 * limitation of getting only 1000 search results.
	 * 
	 * The default page size for fetching pull requests is 100. In case, we have
	 * more than 1000 pull requests in our window, at 11th page it throws an
	 * error.
	 * 
	 * Due to this, we can only paginate from first page till 10th page. For
	 * getting 11th page pull requests, we update aftertime to the updated time
	 * of 100th record found in 10th page, and restart the pagination from 1.
	 * 
	 * This continues till we get all the records between the given aftertime
	 * and maxtime.
	 */
	private GithubPullRequests getPullRequestsInBetweenWithPagination(final Calendar afterTime,
			final Calendar maxTime, final int pageNumberToSet, final String criteriaQuery)
			throws GithubConnectorException {
		Set<GithubPullRequestSummary> allPullRequests = new HashSet<>();
		int pageNumber = pageNumberToSet;
		boolean isNextPageRequired = false;
		GithubPullRequestSummary lastPullRequestSummary = null;
		Calendar tempAfterTime = GithubUtility.getCalendarFromMillis(afterTime.getTimeInMillis());
		do {
			// We are updating aftertime to last pull request, only if the page
			// we are about to get is 1 and if last pull request is not null
			if (pageNumber == 1 && lastPullRequestSummary != null) {
				tempAfterTime.setTimeInMillis(lastPullRequestSummary.getUpdatedAt().getTimeInMillis());
			}
			UrlBuilder urlBuilder = GithubUrlBuilderHandler.getURLBuilderForGetEntitiesChanged(tempAfterTime, maxTime,
					pageNumber, criteriaQuery, getRepoNameFromId());
			CRUDResponseObject crudResponseObject =  sendReadRequest(urlBuilder);
			GithubPullRequests pullRequests = parseResponseObject(urlBuilder, crudResponseObject);
			if (pullRequests != null && CollectionUtils.isNotEmpty(pullRequests.getPullRequests())) {
				// Add all the pull requests for the fetched page in a set.
				allPullRequests.addAll(pullRequests.getPullRequests());
				// Store the last record of the fetched page.
				lastPullRequestSummary = pullRequests.getPullRequests().get(pullRequests.getPullRequests().size() - 1);
			}
			// This method decides the next page number to be fetched. Page
			// number can be from 1 to 10
			int nextPage = getNextPageNumber(pullRequests, pageNumber, crudResponseObject);
			// If next page found is -1, we stop the pagination and return the
			// results found.
			isNextPageRequired = nextPage == -1 ? false : true;
			pageNumber = nextPage;
		} while (isNextPageRequired);
		return new GithubPullRequests(allPullRequests.size(), false,
				allPullRequests.stream().collect(Collectors.toList()));
	}

	/*
	 * This function decides the page number to be fetched according to the
	 * total number of pull requests found in the search result. In the search
	 * result, we get the total count of pull requests present in between the
	 * given aftertime and maxtime.
	 * 
	 * Thus, we decide, if we have to restart the pagination from 1 or continue
	 * the pagination from 1 to 10.
	 * 
	 * If no pagination is required, i.e. if all the records are fetched, then
	 * we return -1.
	 */
	private int getNextPageNumber(final GithubPullRequests pullRequests, final int pageNumber,
			final CRUDResponseObject crudResponseObject) {
		boolean nextPageAvailableFromResponse = checkResponseHeadersForNextPage(crudResponseObject);
		int currentPage = pageNumber;
		if (nextPageAvailableFromResponse) {
			return currentPage + 1;
		} else if (pullRequests != null && pullRequests.isPageAvailbleAfterCurrentPage(currentPage)) {
				return 1;
		}
		return -1;
	}

	private GithubPullRequests parseResponseObject(final UrlBuilder urlBuilder,
			final CRUDResponseObject crudResponseObject) throws GithubConnectorException {
		try {
			return crudResponseObject.getJsonResponseAs(GithubPullRequests.class, true);
		} catch (CRUDRequesterException ex) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00052,
					new String[] { urlBuilder.getUrlBuilt(), ex.getMessage() }, ex);
		}
	}

	private boolean checkResponseHeadersForNextPage(final CRUDResponseObject responseObject) {
		NameValuePair linkResponseHeader = new NameValuePair("", "");
		if (responseObject.getResponseHeaders() != null) {
			linkResponseHeader = responseObject.getResponseHeaders().stream()
					.filter(header -> "Link".equals(header.getKey())).findFirst().orElse(new NameValuePair("", ""));
		}
		return linkResponseHeader.getValue().contains("rel=\"next\"");
	}

	public CRUDResponseObject sendReadRequest(final UrlBuilder urlBuilder) throws GithubConnectorException {
		return sendGenericRequest(urlBuilder, null, MethodType.GET);
	}

	public void setInterrupted() {
		retryJobInterrupted = true;
	}

	public boolean isRetryInterrupted() {
		return retryJobInterrupted;
	}

	public void handleInterruptedException(final GithubConnectorException ex) {
		if (ex instanceof GithubConnectorInterruptedException) {
			return;
		} else {
			throw new OIMRunTimeException(
					"Error occurred while fetching entities due to : " + ex.getLocalizedMessage(), ex);
		}
	}

	public String sendPOSTRequestForMarkDown(final UrlBuilder urlBuilder, final String requestBody)
			throws GithubConnectorException {
		try {
			CRUDResponseObject responseObject = sendGenericRequest(urlBuilder, requestBody, MethodType.POST);
			return responseObject.getResponseAsString();
		} catch (CRUDRequesterException ex) {
				throw new GithubConnectorException(GithubConstants.ErrorCode._00052,
						new String[] { urlBuilder.getUrlBuilt(), ex.getMessage() }, ex);
		}
	}

	private CRUDResponseObject sendGenericRequest(final UrlBuilder urlBuilder, final String requestBody,
			final MethodType methodType, final String attachmentURL) throws GithubConnectorException {
		String requestURL = "";
		try {
			CRUDResponseObject crudResponseObject = sendRequest(urlBuilder, requestBody, methodType, attachmentURL);
			requestURL = urlBuilder == null ? attachmentURL : urlBuilder.getUrlBuilt();
			LOGGER.debug("Sent request with URL - " + requestURL);
			checkResponseHeadersForApiRateLimit(crudResponseObject);
			boolean isRetry = checkStatusCodeAndRetry(crudResponseObject, requestURL);
			if (isRetry) {
				// This will get response header and wait till reset time
				checkResponseHeadersForApiRateLimit(crudResponseObject);
				return sendGenericRequest(urlBuilder, requestBody, methodType, attachmentURL);
			}
			return crudResponseObject;
		} catch (CRUDRequesterException | InterruptedException ex) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00052,
					new String[] { requestURL, ex.getMessage() }, ex);
		}
	}

	private CRUDResponseObject sendGenericRequest(final UrlBuilder urlBuilder, final String requestBody,
			final MethodType methodType) throws GithubConnectorException {
		return sendGenericRequest(urlBuilder, requestBody, methodType, "");
	}

	private CRUDResponseObject sendRequest(final UrlBuilder urlBuilder, final String requestBody,
			final MethodType methodType, final String attachmentPlainURL) throws CRUDRequesterException {
		return MethodType.GET.equals(methodType)
				? urlBuilder == null ? crudRequester.readPlainRequest(attachmentPlainURL)
						: crudRequester.readRequest(urlBuilder)
				: crudRequester.genericPostRequest(urlBuilder, requestBody, Collections.emptyList());
	}

	public InputStream getInputStreamFromURL(final String attachmentURL) throws GithubConnectorException {
		try {
			LOGGER.debug("Get Request Sent For : " + attachmentURL);
			CRUDResponseObject responseObject = null;
			if (!attachmentURL.contains("user-images.githubusercontent.com"))
				responseObject = sendGenericRequest(null, null, MethodType.GET, attachmentURL);
			else {
				responseObject = CommonCrudRequester.newBuilder().noAuthentication(restUrl).build()
						.readPlainRequest(attachmentURL);
			}
			LOGGER.debug("Response Status Code for  : " + attachmentURL + " is " + responseObject.getStatusCode());
			return responseObject.getResponseAsInputStream();
		} catch (CRUDRequesterException ex) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00052,
					new String[] { attachmentURL, ex.getMessage() }, ex);
		}
	}

	public GithubUser getUserMetaFromAPI(final String userName) throws GithubConnectorException {
		UrlBuilder urlBuilder = GithubUrlBuilderHandler.getURLForUser(userName);
		return sendReadRequestWithoutPagination(urlBuilder, new TypeReference<GithubUser>() {});
	}

	/**
	 * This method is getter method for connector context of the requester
	 * 
	 * @return
	 */
	public GithubConnectorContext getGithubConnectorContext() {
		return this.connectorContext;
	}

	/**
	 * This method retrieves all the data after building an url based on the url
	 * and query parameters passed
	 * 
	 * @param typeReference
	 * @param whereClause
	 * @param parts
	 * @return
	 * @throws GithubConnectorException
	 * @throws CRUDRequesterException
	 */
	public Object getDataWithoutPagination(final TypeReference<?> typeReference, final HashMap<String, String> whereClause, final String... parts) throws GithubConnectorException, CRUDRequesterException {
		// URL and Query builder and instantiated to create a URL for fetching
		// the data
		QueryBuilder queryBuilder = null;
		List<Object> allData = new ArrayList<>();
		CRUDResponseObject crudResponseObject = null;
		List<Object> data = null;
		UrlBuilder urlBuilder = UrlBuilder.newUrlBuilder();
		queryBuilder = QueryBuilder.newQueryBuilder();
		// The map of where clause is iterated over and all the conditions are
		// added as generic clauses
		if (whereClause != null) {
			Iterator<Entry<String, String>> itrOnWhereClause = whereClause.entrySet().iterator();
			while (itrOnWhereClause.hasNext()) {
				Entry<String, String> entry = itrOnWhereClause.next();
				queryBuilder.genericClause(entry.getKey(), entry.getValue());
			}
		}
		// For each generic clause, the addition is made to the list of all
		// query conditions
		List<String> listOfParts = new ArrayList<String>();
		for (String part : parts) {
			if (part != null && part.contains("/")) {
				String[] numberOfParts = part.split("/");
				listOfParts.addAll(Arrays.asList(numberOfParts));
			} else {
				listOfParts.add(part);
			}
		}
		// URL is built with all the query conditions
		urlBuilder.addUrlParts(listOfParts.toArray(new String[0]));
		urlBuilder.addQueryParts(queryBuilder);
		// Read request is sent using the build URL with the required conditions
		crudResponseObject = sendReadRequest(urlBuilder);
		// The response is fetched and converted to the type reference passed in
		// the parameter
		data = (List<Object>) GithubUtility.convertToObject(crudResponseObject.getResponseAsString(), typeReference);
		data = data != null ? data : Collections.emptyList();
		allData.addAll(data);
		return allData;
	}
}