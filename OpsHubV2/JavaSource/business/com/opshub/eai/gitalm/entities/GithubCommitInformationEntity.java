/**
 * Copyright C 2019 OpsHub, Inc. All rights reserved
 */
package com.opshub.eai.gitalm.entities;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.opshub.eai.EAIComment;
import com.opshub.eai.EAIEntityRefrences;
import com.opshub.eai.EAIKeyValue;
import com.opshub.eai.EaiUtility;
import com.opshub.eai.OIMCriteriaStorageInformation;
import com.opshub.eai.core.carriers.EntityPrimaryDetailsCarrier;
import com.opshub.eai.core.carriers.MaxEntityCarrier;
import com.opshub.eai.core.carriers.NonHistoryBasedInfoCarrier;
import com.opshub.eai.core.carriers.ProcessingCarrier;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.gitalm.adapter.GithubConnector;
import com.opshub.eai.gitalm.common.GithubConstants;
import com.opshub.eai.gitalm.common.GithubConstants.GitALMFieldNames;
import com.opshub.eai.gitalm.common.GithubRestRequester;
import com.opshub.eai.gitalm.common.SourceFieldsMetaData;
import com.opshub.eai.gitalm.data.Branches;
import com.opshub.eai.gitalm.data.Commit;
import com.opshub.eai.gitalm.exceptions.GithubConnectorException;
import com.opshub.eai.gitalm.exceptions.OIMGithubPollerException;
import com.opshub.eai.gitalm.poller.GithubCommitInformationEventHandler;
import com.opshub.eai.gitalm.poller.GithubCommitInformationEventHandler.GithubCommitInformationWindow;
import com.opshub.eai.metadata.AttachmentMeta;
import com.opshub.eai.metadata.CommentsMeta;
import com.opshub.eai.metadata.DataType;
import com.opshub.eai.metadata.ExtendedCommentsMeta;
import com.opshub.eai.metadata.FieldsMeta;
import com.opshub.exceptions.eai.EAIPollerException;
import com.opshub.exceptions.eai.EAIProcessException;
import com.opshub.logging.OpsHubLoggingUtil;
import com.opshub.scmclients.LogMessageBean;


public class GithubCommitInformationEntity implements GithubEntityHandler {

	private static final String UNCHECKED_ANNOTATION = "unchecked";
	private final GithubRestRequester githubRestRequester;
	private static final String USERNAME = "UserName";

	private final GithubConnectorService connectorService;
	private static final Logger LOGGER = Logger.getLogger(GithubCommitInformationEntity.class);
	
	private GithubCommitInformationEventHandler commitEventHandler;

	public GithubCommitInformationEntity(final GithubRestRequester githubRestRequester,
			final GithubConnectorService connectorService) {
		this.githubRestRequester = githubRestRequester;
		this.connectorService = connectorService;
		this.commitEventHandler = new GithubCommitInformationEventHandler(this);
	}

	public GithubRestRequester getGithubRestRequester() {
		return this.githubRestRequester;
	}

	public GithubConnectorService getGithubConnectorService() {
		return this.connectorService;
	}

	@Override
	public List<FieldsMeta> getFieldsMetadata() {
		List<FieldsMeta> fieldsMeta = SourceFieldsMetaData.getFieldsMetadata();
		fieldsMeta.add(new FieldsMeta(GitALMFieldNames.BRANCHNAME, GitALMFieldNames.BRANCHNAME, DataType.TEXT, false,
				true, false));
		return fieldsMeta;

	}

	@Override
	public CommentsMeta getCommentsMeta() {
		CommentsMeta commentMeta = new CommentsMeta();
		commentMeta.setSupportAsTargetSystem(true);
		commentMeta.setSupportAsSourceSystem(true);
		return commentMeta;

	}

	@Override
	public List<EAIKeyValue> getAllCommentTypes() {
		return Collections.emptyList();
	}

	/**
	 * if entity type is commit information, then get max time from branch,
	 * fetch all branch which returns max commit ids, iterate each ids and get
	 * max date from it and returns
	 */
	@Override
	public MaxEntityCarrier getMaxUpdateTime(final Calendar afterTime) throws GithubConnectorException {
		Calendar returnAfterTime = Calendar.getInstance();
		returnAfterTime.setTimeInMillis(afterTime.getTimeInMillis());
		String sha = "";
		// fetch repository Name from id
		String repoName = githubRestRequester.getRepoNameFromId();
		// fetch all branches
		List<Branches> listOfBranches = connectorService.getAllBranches(true);
		// iterate to all branch and get
		for (Branches branches : listOfBranches) {
			Commit commit = githubRestRequester.getCommitInformation(repoName, branches.getCommit().getSha());
			if (commit != null && commit.getCommit() != null && commit.getCommit().getCommitter() != null
					&& commit.getCommit().getCommitter().getDate() != null) {
				branches.setBranchLastUpdatedTime(commit.getCommitedDate());
				if (commit.getCommitedDate().compareTo(returnAfterTime)>=0) {
					returnAfterTime.setTimeInMillis(commit.getCommitedDate().getTimeInMillis());
					sha = branches.getCommit().getSha();
				}
			}
		}
		connectorService.updateBranchDataInCache(listOfBranches);
		return new MaxEntityCarrier(sha, returnAfterTime, "");
	}



	@Override
	public Object getEntityObject(final String internalId) throws GithubConnectorException {
		return getLogMessageBean(internalId);
	}

	private Map<String, Object> getLogMessageBean(final String commitId) throws GithubConnectorException {
		Commit commit = githubRestRequester.getCommitInformation(githubRestRequester.getRepoNameFromId(), commitId);
		LogMessageBean logBean = githubRestRequester.getLogMessagebean(commit);
		List<EAIKeyValue> listOfEaiKeyValue = null;
		try {
			listOfEaiKeyValue = EaiUtility.getEAIKeyValueFromInstance(logBean);
		} catch (EAIProcessException e) {
			throw new GithubConnectorException(GithubConstants.ErrorCode._00036, new String[] { commitId }, e);
		}
		// changes for author and commit date meta information
		Map<String, Object> data = EaiUtility.eaiKeyValueToMap(listOfEaiKeyValue);

		String comment = (String) data.get(GithubConstants.RestRequesterConstants.COMMENT);
		String regEx = connectorService.getGithubConnectorContext().getRegExpression();
		String issueId = GithubRestRequester.getTargetEntityIdByFindingRegex(comment, regEx);
		data.put(SourceFieldsMetaData.LINKED_WORKITEM, issueId);
		data.put(SourceFieldsMetaData.COMMITDATE, commit.getCommitedDate());
		data.put(SourceFieldsMetaData.AUTHOR, commit.getCommit().getAuthorUserMeta());
		return data;
	}

	@SuppressWarnings(UNCHECKED_ANNOTATION)
	@Override
	public Map<String, Object> getSystemProperties(final String internalId, final Object entityObject)
			throws GithubConnectorException {
		if (entityObject == null) {
			return (Map<String, Object>) getEntityObject(internalId);
		}
		return (Map<String, Object>) entityObject;
	}

	@Override
	public Object getPropertyValue(final String internalId, final String propertyName)
			throws GithubConnectorException {
		return null;
	}

	@Override
	public Iterator<? extends ProcessingCarrier> getEntitiesChangedAfter(final Calendar afterTime,
			final Calendar maxTime, final String lastProcessedId,
			final OIMCriteriaStorageInformation criteriaStorageInfo, final GithubConnector githubConnector) throws OIMGithubPollerException {
		GithubCommitInformationWindow commitInformationWindow = commitEventHandler.new GithubCommitInformationWindow(
				afterTime, maxTime, connectorService.getGithubConnectorContext().getPollingWindowSize(),
				lastProcessedId);
		LOGGER.debug("Commit Information Window created with start time: " + afterTime.getTime().toString()
				+ " and finish time is: " + maxTime.getTime().toString());
		return commitEventHandler.new GithubCommitInformationEventIterator(commitInformationWindow, githubConnector);
	}

	@SuppressWarnings(UNCHECKED_ANNOTATION)
	@Override
	public Map<String, Object> getCurrentState(final ProcessingCarrier entityDetails) throws OIMGithubPollerException {
		try {
			return (HashMap<String, Object>) getEntityObject(entityDetails.getInternalId());
		} catch (OIMAdapterException e) {
			throw new OIMGithubPollerException(GithubConstants.ErrorCode._017430,
					new String[] { entityDetails.getInternalId() }, e);
		}
	}

	@Override
	public Iterator<EntityPrimaryDetailsCarrier> getAllEntitiesMeetingCriteria(final String query,
			final Calendar updatedAfterTime, final Calendar updatedbeforeTime)
			throws OIMGithubPollerException {
		OpsHubLoggingUtil.error(LOGGER,
				"Criteria is not supported for entity " + GithubConstants.GitALMEntityNames.COMMIT, null);
		throw new OIMGithubPollerException(GithubConstants.ErrorCode._017429,
				new String[] { GithubConstants.GitALMEntityNames.COMMIT }, null);
	}

	@Override
	public EAIEntityRefrences getCurrentLinks(final String internalId, final String linkType)
			throws GithubConnectorException {
		// Links are not supported for Commit information
		return null;
	}

	@Override
	public List<EAIEntityRefrences> getLink(final NonHistoryBasedInfoCarrier entityDetails, final Object currentState)
			throws OIMGithubPollerException {
		// Links are not supported for Commit information
		return null;
	}

	@Override
	public String getRemoteEntityLink(final String id) throws GithubConnectorException {
		Commit commit = githubRestRequester.getCommitInformation(githubRestRequester.getRepoNameFromId(), id);
		return commit.getHtml_url();
	}

	@Override
	public ExtendedCommentsMeta getExtendedCommentsMeta() {
		return new ExtendedCommentsMeta(getCommentsMeta(), false, false);
	}

	@Override
	public List<EAIComment> getComments(final String entityID, final String afterCommentID, final Calendar afterTime)
			throws GithubConnectorException {
		// as commit having only one comment which is message, hence returning
		// the comment without checking the after time. as from core in case its
		// time is <= event time then only its get processed
		// further message time will be always equal to event time
		Commit commit = githubRestRequester.getCommitInformation(githubRestRequester.getRepoNameFromId(), entityID);
		List<EAIComment> eaiComment = new ArrayList<EAIComment>();
		if (commit != null && commit.getCommit().getMessage()!=null) {
			final EAIComment comment = new EAIComment("", "", "", "", commit.getCommit().getMessage(),
					commit.getCommitedDate().toString(), commit.getCommitedDate().toString(),
					commit.getCommit().getUserMeta() != null ? commit.getCommit().getUserMeta().getUserName() : null);
			comment.setCreatedOrUpdated(new Timestamp(commit.getCommitedDate().getTimeInMillis()));
			eaiComment.add(comment);
		}
		return eaiComment;
	}

	/**
	 * Returning attachment meta for only source as true as inline image and
	 * document will get polled in case of Github pull request entity
	 */
	@Override
	public AttachmentMeta getAttachementMeta() {
		AttachmentMeta attachmentMeta = new AttachmentMeta();
		attachmentMeta.setAsSourceSupported(false);
		attachmentMeta.setAsTargetSupported(false);
		attachmentMeta.setDeleteSupported(false);
		attachmentMeta.setHistorySupported(false);
		attachmentMeta.setUpdateSupported(false);
		return attachmentMeta;
	}

	@Override
	public InputStream getAttachmentInputStream(final String attachmentURI) throws OIMAdapterException {
		LOGGER.trace("Attachment is not supported for Commit Information entity.");
		throw new UnsupportedOperationException("Attachment is not supported for Commit Information entity");
	}

	@Override
	public String getCreatedBy(final ProcessingCarrier entityDetails, final Object entityCurrentState,
			final String entityID) throws EAIPollerException {
		// Return the user who committed in github
		if (entityCurrentState != null) {
			return (String) ((HashMap<String, Object>) entityCurrentState).get(USERNAME);
		} else {
			try {
				return (String) ((HashMap<String, Object>) getEntityObject(entityID)).get(USERNAME);
			} catch (OIMAdapterException e) {
				throw new EAIPollerException(GithubConstants.ErrorCode._017431,
						new String[] { entityDetails.getInternalId() }, e);
			}
		}
	}

}
