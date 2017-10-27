import groovy.json.JsonSlurper
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.Method
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import java.util.regex.Matcher

@Grapes([
        @Grab(group = 'org.jsoup', module = 'jsoup', version = '1.10.3'),
        @Grab(group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.7.1')
])

List<String> categories = ["automotive-manufacturers-jobs"]

scrapeCategories(categories[0])

static def scrapeCategories(String category) {
    List<JobListing> jobListings = []
    int index = 0
    PageInfo info = extractInfo(category, index)
    jobListings.addAll(info.jobListings)
    Random r = new Random()
    while (info.nextButtonExists) {
        index++
        if(index > 1) {
            break
        }
        Thread.sleep(1000 + r.nextInt(2000))
        try {
            info = extractInfo(category, index)
            jobListings.addAll(info.jobListings)
        } catch (Exception e) {
            println("Unable to extract info for index $index")
            e.printStackTrace()
        }
    }
    extractAndWriteCompanies(jobListings)
}

private static Object appendCompanyToFile(Company company, String fileName) {
    File file = new File(fileName)
    if(!file.exists()) {
        new FileWriter(fileName).withPrintWriter {
            it.write("Company Name,Email,Alternate Company Name,Recruiter Name,Website,Telephone,Normalized Hostname,Company Type\n")
        }
    }
    new FileWriter(fileName).withPrintWriter {
        it.append(company.toString() + "\n")
    }
}

private static PageInfo extractInfo(String category, int index) {
    String url = getUrl(category, index)
    String docHtml = getDocument(url)
    Document doc = Jsoup.parse(docHtml)
    Elements buttons = getNavigationButtons(doc)
    boolean nextButtonExists = buttons.any {
        it.text().equalsIgnoreCase("Next")
    }
    Elements results = doc.select("div.mainSec div.srp_container > div.row > a")
    List<JobListing> listings = results.collect { Element elem ->
        String companyName = elem.select("span.org").text()
        String href = elem.attr("abs:href")
        new JobListing(href, companyName)
    }
    new PageInfo(nextButtonExists, listings)
}

private static Elements getNavigationButtons(Document doc) {
    doc.select("div.pagination button.grayBtn")
}

private static String getDocument(String url, int retryCount = 0) {
    println("${new Date().toString()}: Fetching url $url")
    String html = ""

    try {
        HTTPBuilder httpBuilder = new HTTPBuilder(url)
        httpBuilder.request(Method.GET, ContentType.TEXT, { req ->
            headers.'Accept' = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            headers.'Accept-Language' = 'en-US,en;q=0.5'

            response.success = { resp, Reader reader ->
                html = reader.text
            }

            response.failure = { resp ->
                println("Failed ${resp.status}")
            }
        })
        html
    } catch (Exception e) {
        e.printStackTrace()
        Thread.sleep(2000)
        if (retryCount > 5) {
            throw new IllegalStateException()
        }
        html = getDocument(url, ++retryCount)
    }

    return html
}

static String getUrl(String category, int index) {
    "http://www.nau" + "kri.com/$category" + (index == 0 ? "" : "-$index")
}

@TupleConstructor
class PageInfo {
    boolean nextButtonExists
    List<JobListing> jobListings
}

@TupleConstructor
@ToString
@EqualsAndHashCode
class JobListing {
    String url
    String companyName
}

@TupleConstructor
@EqualsAndHashCode
class Company {
    String name
    String email
    String contactCompanyName
    String recruiterName
    String website
    String telephone
    String normalizedHostName
    String companyType

    @Override
    String toString() {
        "${q(name)},${q(email)},${q(contactCompanyName)},${q(recruiterName)},${q(website)},${q(telephone)},${q(normalizedHostName)},$companyType"
    }

    static String q(String val) {
        if (val.indexOf(",") != -1) {
            val = val.replaceAll("\"", "\"\"")
            val = "\"$val\""
        }
        return val
    }
}

static def extractAndWriteCompanies(List<JobListing> jobListings) {
    String fileName = "companies-${System.currentTimeSeconds()}.csv"
    Random r = new Random()
    jobListings.groupBy { JobListing jobListing ->
        jobListing.companyName
    }.collect { Map.Entry<String, List<JobListing>> listings ->
        Company company = listings.value.unique().withIndex().findResult { JobListing listing, int index ->
            println("Getting job listing for company ${listings.key} and index $index")
            String jobListingId = getJobListingId(listing.url)
            Thread.sleep(1000 + r.nextInt(2000))
            try {
                return getCompanyFromJobListingId(jobListingId, listing, listings.value.size() - 1 == index)
            } catch(Exception e) {
                println("Exception with $jobListingId")
                e.printStackTrace()
                return null
            }
        } as Set
        appendCompanyToFile(company, fileName)
    }
}

private static Company getCompanyFromJobListingId(String jobListingId, JobListing listing, boolean lastListing) {
    try {
        def fields = getContactDetailsFields(jobListingId)
        if(!fields) {
            return null
        }

        // Not important fields
        String recruiterName = fields.'Recruiter Name' ?: ""
        String contactCompanyName = fields.'Contact Company' ?: "" // optional
        String telephone = fields.'Telephone' ?: "" // optional
        String referenceId = fields.'Reference Id' ?: "" // optional

        // Important fields
        def (String website, String email) = getWebsiteAndEmail(fields)
        String companyType = getCompanyType(listing.companyName)

        if (!website) {
            // This is the last listing, so we have to return a value no matter what
            if (lastListing) return new Company(listing.companyName, email, contactCompanyName, recruiterName, website, telephone, "", companyType)
            else return null
            // Return null in the hope that the next element will help us find the website
        } else {
            String normalizedHostName = getNormalizedHostName(website)
            return new Company(listing.companyName, email, contactCompanyName, recruiterName, website, telephone, normalizedHostName, companyType)
        }
    } catch (Exception e) {
        println("Exception with $listing")
        e.printStackTrace()
        return null
    }
}

private static List getWebsiteAndEmail(fields) {
    String website = fields.'Website' ?: "" // optional
    if (website) {
        website = website.replaceAll(" ", "")
    }
    def emailObj = fields.'Email Address'
    String email = ""
    if (emailObj) {
        email = emailObj.title // optional
        if (!website) {
            website = getWebsiteFromEmail(email)
        }
    }
    [website, email]
}

private static def getContactDetailsFields(String jobListingId) {
    String html = getDocument("https://www.nau" + "kri.com/jd/contactDetails?file=$jobListingId")
    JsonSlurper jsonSlurper = new JsonSlurper()
    def result = jsonSlurper.parseText(html)
    result.'fields'
}

static String getJobLinastingId(String url) {
    def pattern = ~/https.+-(\d{7,15})(\?src=.+)?/
    Matcher m = pattern.matcher(url)
    if (!m.matches()) {
        throw new IllegalArgumentException("Invalid regex")
    }
    m.group(1)
}

static String getWebsiteFromEmail(String email) {
    List<String> emails = email.split(",").collect { it.trim() }
    String firstEmail = emails[0]
    def p = ~/.+@(.+)/
    Matcher matcher = p.matcher(firstEmail)
    if (!matcher.matches()) {
        throw new IllegalArgumentException()
    }
    matcher.group(1).trim()
}

static String getCompanyType(String companyName) {
    List<String> wordsToIgnore = ["Consultancy", "Immigration", "Consultants", "Consulting", "EMPLOYMENT", "Placement", "Manpower", "HR", "CONFIDENTIAL", "Talent", "Placements", "Placement", "Hiring", "Hirings", "Premium", "Search", "Recruitment", "Agency", "Management"]
    String[] companyNameWords = companyName.split(" ")
    int filteredWordsSize = companyNameWords.findAll { String word ->
        !wordsToIgnore.any {
            it.equalsIgnoreCase(word)
        }
    }.size()
    boolean filteredWordExists = filteredWordsSize != companyNameWords.size()
    if (filteredWordExists) {
        return "HR"
    } else {
        return "NonHR"
    }
}

static String getNormalizedHostName(String website) {
    def p = ~/(?:http:\/\/)?(?:www\.)?(.+?)(?:\/)?/
    Matcher m = p.matcher(website)
    if (!m.matches()) {
        throw new IllegalArgumentException()
    } else {
        m.group(1).trim()
    }
}